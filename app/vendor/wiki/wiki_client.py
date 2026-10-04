"""Клиент Yandex Wiki API v2.

Вся корректность работы с API инкапсулирована здесь: пагинация листовых
эндпоинтов, форма ответов, обязательные параметры, асинхронные операции.
Для CommonMark-блочного разбора лениво используется markdown-it-py; импорт
клиента и операции, не связанные с заголовками, остаются stdlib-only.

Использование:

    from wiki_client import WikiClient
    w = WikiClient()                       # токен из окружения, см. _resolve_wiki_token
    page = w.get_page(slug="stefania", fields=["content", "authors"])
    pages = w.descendants_titled(page["id"], "stefania")  # полный список с title
"""

import http.client
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import warnings
from concurrent.futures import ThreadPoolExecutor

# Базовый путь без завершающего `/`: внутренние маршруты уже начинаются с `/`,
# поэтому прокси-префикс `/wiki` сохраняется и не удваивается.
BASE_URL = "https://agentproxy.yandex.net/wiki"
DIRECT_BASE_URL = "https://wiki-api.yandex-team.ru"

# Только известные маршруты, которых нет в Agent Proxy. Выбор делается до
# отправки: отказ Proxy (включая processor_denied) не разрешает direct retry.
# Остальные, в том числе новые неизвестные маршруты, остаются в Proxy.
_DIRECT_ROUTES = (
    ("GET", r"pages/(?:autocomplete|suggest_slug)"),
    ("POST", r"pages/move"),
    ("GET", r"pages/-?[1-9][0-9]*/(?:backlinks|inheritable_acl|subscribers)"),
    (
        "POST",
        r"pages/-?[1-9][0-9]*/(?:clone|access|grant_author_role|change_order|"
        r"subscribers|change_to_yfm)",
    ),
    ("DELETE", r"pages/-?[1-9][0-9]*/access(?:/[A-Za-z0-9_-]+)?"),
    ("DELETE", r"pages/-?[1-9][0-9]*/subscribers/[A-Za-z0-9_-]+"),
    ("POST", r"pages/drafts"),
    ("POST", r"pages/drafts/[A-Za-z0-9_-]+/publish"),
    ("GET", r"recovery_tokens"),
    ("POST", r"recovery_tokens/[A-Za-z0-9_-]+/recover"),
    ("POST", r"grids/compat"),
    ("POST", r"grids/[A-Za-z0-9_-]+/clone"),
    ("GET", r"grids/[A-Za-z0-9_-]+/revisions(?:/diff)?"),
    ("GET", r"navtree/load_next"),
    (
        "GET",
        r"operations/(?:move|clone|clone_grid|clone_inline_grid)/[A-Za-z0-9_-]+",
    ),
)
_DIRECT_ROUTE_PATTERNS = tuple(
    (method, re.compile(r"/api/v2/public/" + route, re.ASCII))
    for method, route in _DIRECT_ROUTES
)


def _request_url(path, method):
    """Выбрать транспорт по точным method/path, сохранив исходный query."""
    parsed = urllib.parse.urlsplit(path)
    if (
        not path.startswith("/")
        or path.startswith("//")
        or parsed.scheme
        or parsed.netloc
        or parsed.fragment
        or any(ord(char) < 32 or ord(char) == 127 for char in path)
    ):
        raise ValueError("Wiki API path must be an absolute path without a host")
    direct = any(
        method == allowed_method and pattern.fullmatch(parsed.path)
        for allowed_method, pattern in _DIRECT_ROUTE_PATTERNS
    )
    return f"{DIRECT_BASE_URL if direct else BASE_URL}{path}"


def _new_markdown_block_parser():
    """Создать parser только при первой операции с заголовками.

    Импорт намеренно локальный: отсутствие pip-зависимости не должно ломать
    CRUD WikiClient на этапе импорта модуля.
    """
    from markdown_it import MarkdownIt

    return MarkdownIt("commonmark", {"html": True})


def _service_uses_robot(service):
    """wiki настроена на робот-аккаунт? Источник правды — STEFANIA_ROBOT_SERVICES
    (csv в customize.env). Нет переменной / нет сервиса в списке → личный аккаунт."""
    svcs = {
        s.strip()
        for s in os.environ.get("STEFANIA_ROBOT_SERVICES", "").split(",")
        if s.strip()
    }
    return service in svcs


def _resolve_wiki_token():
    """Личный по умолчанию; робот-первым — только если 'wiki' в
    STEFANIA_ROBOT_SERVICES. Второй — фолбэк (если первый не задан)."""
    personal = os.environ.get("WIKI_TOKEN", "").strip()
    robot = os.environ.get("WIKI_ROBOT_TOKEN", "").strip()
    if _service_uses_robot("wiki"):
        return robot or personal
    return personal or robot


class WikiAPIError(RuntimeError):
    """Ошибка Wiki API. Несёт http-код и error_code из тела ответа."""

    def __init__(self, status, error_code, message):
        self.status = status
        self.error_code = error_code
        super().__init__(f"Wiki API {status}: {error_code} — {message}")


class WikiClient:
    """Тонкий типизированный клиент Wiki API.

    Листовые эндпоинты (descendants, attachments, backlinks, revisions,
    comments, thread, subscribers, grids, recovery_tokens) всегда возвращают
    ПОЛНЫЙ список — пагинация скрыта внутри. Исключение — autocomplete:
    курсора у него нет, выдача усечена потолком эндпоинта (100).
    """

    def __init__(self, token=None, timeout=60):
        self._token = token or _resolve_wiki_token()
        if not self._token:
            raise RuntimeError(
                "WIKI_TOKEN не задан. Экспортируй OAuth-токен Yandex Wiki "
                "в переменную окружения WIKI_TOKEN (или передай token=...). "
                "WIKI_ROBOT_TOKEN используется первым, только если 'wiki' есть "
                "в $STEFANIA_ROBOT_SERVICES."
            )
        self._timeout = timeout

    # --- транспорт -----------------------------------------------------

    # error_code, которые имеет смысл ретраить (сервис временно недоступен)
    _RETRY_CODES = {"SERVICE_IS_READONLY", "BAD_GATEWAY", "TEMPORARILY_UNAVAILABLE"}

    # Повтор безопасен только для чтения. Для записи неопределённый исход
    # (5xx, обрыв, таймаут чтения ответа) может означать «уже применено» —
    # повтор создаст дубль страницы/комментария. Запись повторяем только
    # там, где сервер её точно НЕ применил: SERVICE_IS_READONLY и 429
    # (rate limiter отвергает запрос до обработки).
    # DELETE/PUT формально идемпотентны, но здесь намеренно в одной корзине
    # с POST: цена ошибки (повторное удаление поддерева) выше цены отказа.
    _SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

    def _request(self, path, method="GET", data=None, raw=False, _retries=3):
        """Запрос к API: ошибки → WikiAPIError, ретраи по типу сбоя.

        Чтение повторяется при 429, 5xx, _RETRY_CODES и любом сетевом сбое.
        Запись (не _SAFE_METHODS) — только при 429 и SERVICE_IS_READONLY:
        при обрыве или таймауте ответа сервер мог её уже применить.
        """
        url = _request_url(path, method)
        body = (
            json.dumps(data, ensure_ascii=False).encode("utf-8")
            if data is not None
            else None
        )
        headers = {"Authorization": f"OAuth {self._token}"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        attempts = max(1, _retries)
        for attempt in range(attempts):
            try:
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    payload = resp.read()
                    if raw:
                        return payload
                    return json.loads(payload) if payload else {}
            except urllib.error.HTTPError as e:
                try:
                    err_body = e.read().decode("utf-8", errors="replace")
                except (OSError, http.client.HTTPException) as read_err:
                    # Тело ошибки оборвалось на чтении. Обработчик ниже это
                    # не поймает (мы уже внутри except), и наружу ушёл бы
                    # голый ConnectionResetError мимо WikiAPIError и мимо
                    # ретраев чтения. Статус известен — дальше решает та же
                    # логика по e.code, что и для ответа с целым телом.
                    code, msg = "NETWORK", str(read_err)
                else:
                    try:
                        err = json.loads(err_body)
                        if isinstance(err, dict):
                            # Wiki возвращает error_code, а Agent Proxy — error;
                            # принимаем только непустые строки и сохраняем
                            # приоритет Wiki.
                            code = err.get("error_code")
                            if not isinstance(code, str) or not code:
                                code = err.get("error")
                            if not isinstance(code, str) or not code:
                                code = "?"
                            msg = err.get("debug_message", err_body)
                        else:
                            code, msg = "?", err_body[:500]
                    except json.JSONDecodeError:
                        code, msg = "?", err_body[:500]
                # 4xx (кроме 429 и ретраибельных кодов) — повтор не поможет.
                # 429 обязателен: добор title идёт в _TITLE_POOL потоков, и
                # без повтора один rate-limit роняет всё перечисление раздела.
                transient = (
                    e.code == 429 or e.code >= 500 or code in self._RETRY_CODES
                )
                if transient and method not in self._SAFE_METHODS:
                    transient = e.code == 429 or code == "SERVICE_IS_READONLY"
                if not transient or attempt == attempts - 1:
                    raise WikiAPIError(e.code, code, msg) from None
            except (OSError, http.client.HTTPException) as e:
                # Сетевые сбои после отправки запроса. URLError (connect),
                # голый TimeoutError (таймаут на resp.read() — мимо
                # URLError), ConnectionResetError и IncompleteRead (обрыв
                # посреди тела) — всё это OSError/HTTPException. Для записи
                # исход неопределён: сервер мог её уже применить.
                reason = getattr(e, "reason", e)
                if (
                    method not in self._SAFE_METHODS
                    or attempt == attempts - 1
                ):
                    raise WikiAPIError(0, "NETWORK", str(reason)) from None
            time.sleep(2 * (attempt + 1))

    def _paginate(self, path, page_size=50):
        """Собрать все элементы пагинированного эндпоинта (ключ results).

        page_size=50 — безопасный потолок: /comments отвергает > 50.
        """
        sep = "&" if "?" in path else "?"
        out = []
        url = f"{path}{sep}page_size={page_size}"
        while url:
            resp = self._request(url)
            out.extend(resp.get("results", []))
            cursor = resp.get("next_cursor")
            # Курсор — непрозрачный base64, может содержать +/=; без quote
            # `+` декодируется сервером как пробел и пагинация ломается.
            url = (
                f"{path}{sep}page_size={page_size}"
                f"&cursor={urllib.parse.quote(str(cursor))}"
                if cursor
                else None
            )
        return out

    def _wait_operation(self, kind, op_id, tries=30, interval=2):
        """Дождаться завершения async-операции (clone/clone_grid/
        clone_inline_grid/move). Возвращает финальный статус-объект; статусы:
        scheduled, in_progress, success, failed."""
        status = {}
        for _ in range(tries):
            status = self._request(f"/api/v2/public/operations/{kind}/{op_id}")
            if status.get("status") in ("success", "failed"):
                break
            time.sleep(interval)
        return status

    WIKI_HOST = "wiki.yandex-team.ru"

    @classmethod
    def _split_wiki_url(cls, value):
        """urlsplit c нормализацией «хост без схемы».

        wiki.yandex-team.ru/foo без https:// urlsplit считает путём — slug
        получился бы с хостом внутри (и вёл бы в никуда). Хост без пути
        (`wiki.yandex-team.ru`, `...#anchor`, `...:443/x`) — тот же случай.
        """
        value = value.strip()
        if re.match(rf"(?i)^{re.escape(cls.WIKI_HOST)}([/?#:]|$)", value):
            value = "https://" + value
        return urllib.parse.urlsplit(value)

    @classmethod
    def _slug_from_parts(cls, parsed):
        """Slug из результата urlsplit: path без ?query/#fragment.

        Хост (если есть) сверяется регистронезависимо — hostname у urlsplit
        уже приведён к нижнему регистру, в отличие от поиска подстроки.

        Percent-декодируется только адрес с хостом: ссылка из браузера
        приходит encoded, а методы клиента кодируют slug сами — иначе
        двойное кодирование и NOT_FOUND на живой странице. Готовый slug
        (без схемы и хоста) декодированию НЕ подлежит — второе декодирование
        превратило бы литеральный `%2F` в разделитель пути и увело бы на
        структурно другую страницу. Поэтому метод идемпотентен: свой
        результат можно снова подать и сюда, и в resolve_link_slug /
        descendants_titled.
        """
        if parsed.scheme not in ("", "http", "https"):
            raise ValueError(f"Не Wiki-адрес: схема {parsed.scheme!r}")
        if parsed.netloc:
            if parsed.hostname != cls.WIKI_HOST:
                raise ValueError(f"Ссылка ведёт за пределы {cls.WIKI_HOST}")
            slug = urllib.parse.unquote(parsed.path).strip("/")
            return re.sub(r"/\.diff/\d+$", "", slug)
        if parsed.scheme:
            # http(s) без хоста (`https:///page`) — битый адрес, не slug
            raise ValueError(f"Не Wiki-адрес: {parsed.scheme}:// без хоста")
        return re.sub(r"/\.diff/\d+$", "", parsed.path.strip("/"))

    @classmethod
    def url_to_slug(cls, url):
        """https://wiki.yandex-team.ru/<slug>/ -> <slug>; чужой хост — ValueError.

        Принимает абсолютный Wiki URL (хост регистронезависим, схему можно
        опустить) или уже готовый slug; режет ?query и #fragment. Адрес с
        чужим хостом или не-http схемой отклоняется: молча превратить его
        в «slug» значит потом искать несуществующую страницу.

        Строку без схемы и без хоста метод считает готовым slug'ом и не
        угадывает: `yandex.drive/analytics` — реальный раздел Wiki, отличить
        его от `google.com/x` нечем. Такую строку метод не percent-декодирует
        (иначе литеральный `%2F` стал бы разделителем пути) и потому
        идемпотентен — прогнать свой же результат второй раз безопасно.
        Percent-encoded ссылку отдавай целиком, с хостом. `http(s)://` без
        хоста — ValueError. Относительную ссылку из контента разрешай
        через resolve_link_slug(link, current_slug)."""
        return cls._slug_from_parts(cls._split_wiki_url(url))

    @classmethod
    def resolve_link_slug(cls, link, current_slug):
        """Разрешить Wiki-ссылку из контента относительно текущей страницы.

        Например, для current_slug="section/page" ссылка
        "../templates#card" превращается в "section/templates". Адрес с
        чужим хостом или не-http схемой — ValueError: он не slug Wiki.

        Ссылка без схемы и без хоста разрешается как относительная (RFC
        3986, как в браузере): `st.example.com/x` — автор забыл `https://` —
        даст slug под текущей страницей, и `get_page` честно ответит
        NOT_FOUND. Автолинки голого текста рендер может трактовать иначе —
        для них бери адрес из разметки, а не из отображаемого текста.
        """
        # Ссылку прогоняем через ту же нормализацию, что и url_to_slug:
        # хост без схемы urljoin принял бы за относительный путь и вложил
        # под current_slug — тот же класс тихой ошибки, что и с '../'.
        parsed = cls._split_wiki_url(link)
        if parsed.scheme and not parsed.netloc:
            # Битый адрес (`https:///page`, `mailto:`): urljoin молча
            # достроил бы его до Wiki-страницы, которой в ссылке нет.
            raise ValueError(f"Не Wiki-адрес: {link!r}")
        link = parsed.geturl()
        # url_to_slug отдаёт литеральный slug — кодируем его обратно, иначе
        # _slug_from_parts после urljoin декодирует его ещё раз и «%2F»
        # превратится в разделитель пути, уведя на другую страницу.
        base_slug = urllib.parse.quote(cls.url_to_slug(current_slug))
        base = f"https://{cls.WIKI_HOST}/{base_slug}/"
        return cls._slug_from_parts(
            urllib.parse.urlsplit(urllib.parse.urljoin(base, link))
        )

    # --- чтение --------------------------------------------------------

    def get_page_by_url(self, url, fields=None):
        """Read a Wiki URL, preserving the revision selected by its query.

        A /.diff/<id> suffix is a UI route, not part of the page slug.
        The `revision` query used by the UI maps to API `revision_id`.
        """
        slug = self.url_to_slug(url)
        query = urllib.parse.parse_qs(
            self._split_wiki_url(url).query, keep_blank_values=True
        )
        revisions = query.get("revision", []) + query.get("revision_id", [])
        if len(revisions) > 1:
            raise ValueError("Wiki URL must select only one revision")
        revision_id = None
        if revisions:
            if not re.fullmatch(r"[1-9]\d*", revisions[0]):
                raise ValueError("Wiki URL revision must be a positive integer")
            revision_id = int(revisions[0])
        return self.get_page(slug=slug, fields=fields, revision_id=revision_id)

    def get_page(self, slug=None, page_id=None, fields=None, revision_id=None):
        """Страница по slug или id.

        Поля по умолчанию: id, slug, title, page_type, active_revision.
        Доп. поля через fields=[...]: content, authors, breadcrumbs, owner,
        last_revision_id, access, acl, attributes, actuality, is_readonly...
        revision_id — конкретная историческая ревизия, если задана.

        authors — словарь: {owner, last_author, all: [...], inherited: [...]},
        каждый user = {id, identity:{uid,cloud_uid}, username, display_name,
        is_dismissed, affiliation}. all — назначенные авторы страницы, а не
        история правок: последнего редактора там может не быть (проверено на
        живой странице). Кто реально правил — revisions()/latest_revisions().
        breadcrumbs — список [{id, title, slug, page_exists}] от корня.

        Дефолтные поля (id/slug/title/page_type/active_revision) приходят
        всегда; в параметре fields API их не принимает, поэтому из fields
        они отфильтровываются — передавать их безопасно.
        """
        default = {"id", "slug", "title", "page_type", "active_revision"}
        extra = [f for f in (fields or []) if f not in default]
        q = []
        if revision_id is not None:
            if (
                isinstance(revision_id, bool)
                or not isinstance(revision_id, int)
                or revision_id <= 0
            ):
                raise ValueError("revision_id must be a positive integer")
            q.append(f"revision_id={revision_id}")
        if extra:
            q.append("fields=" + ",".join(extra))
        if slug is not None:
            q.append("slug=" + urllib.parse.quote(slug))
            qs = ("?" + "&".join(q)) if q else ""
            return self._request(f"/api/v2/public/pages{qs}")
        qs = ("?" + "&".join(q)) if q else ""
        return self._request(f"/api/v2/public/pages/{page_id}{qs}")

    def descendants(self, page_id):
        """Полный плоский список потомков. Элемент — {id, slug} (без title)."""
        # page_size=100 — потолок эндпоинта по OpenAPI; дефолтные 50
        # удваивали время костяка на больших разделах (SHARINGML-594).
        return self._paginate(
            f"/api/v2/public/pages/{page_id}/descendants", page_size=100
        )

    def tree(self, slug, max_depth=2):
        """Иерархия раздела: {root: <узел>, page_limit_exceeded: bool}.

        Узел: {id, slug, title, page_type, author, owner, created_at,
        modified_at, children}. На больших разделах появляются узлы-заглушки
        {slug, missing: true, children} без id/title.
        """
        return self._request(
            f"/api/v2/public/pages/tree?slug={urllib.parse.quote(slug)}"
            f"&max_depth={max_depth}"
        )

    _TITLE_POOL = 8  # потоков на добор title; API отвечает ~0.4 с/запрос
    # Оба слоя добора (ветви navtree и хвостовые get_page) идут волнами по
    # _TITLE_BATCH: ex.map ставит в очередь весь итерируемый сразу, и сбой
    # на первом элементе не отменил бы сотни-тысячи уже отправленных
    # запросов — после исчерпанного 429 это fan-out по тому же API. Волна
    # шире пула осознанно: на её границе ждём самый медленный запрос (один
    # ретрай 429 держит всю волну), и волна в размер пула превращала бы это
    # ожидание в накладной расход на каждые 8 элементов — ровно та
    # пропускная способность, ради которой делался SHARINGML-594.
    _TITLE_BATCH = 4 * _TITLE_POOL

    def _navtree_children(self, parent_slug):
        """Дети узла из navtree (API сайдбара) — элемент уже несёт title.

        Форма: [{id, slug, title, has_children, ...}], батчи с курсором.
        page_size=50 — потолок эндпоинта (100 даёт VALIDATION_ERROR),
        задан явно, чтобы не зависеть от дефолта _paginate.
        """
        q = urllib.parse.quote(parent_slug, safe="")
        return self._paginate(
            f"/api/v2/public/navtree/load_next?parent_slug={q}", page_size=50
        )

    def _harvest_titles_via_navtree(self, base_slug, missing, id2title):
        """BFS по navtree только в ветви, где остались страницы без title.

        Полноту гарантирует костяк из /descendants — navtree здесь лишь
        дешёвый источник title (батч по 50 vs один get_page на страницу).
        Закрытая или удалённая ветвь (403/404) молча достаётся хвосту
        get_page, а сбой сервиса (429/5xx/сеть) пробрасывается: после
        исчерпанного rate limit фолбэк означал бы тысячи точечных get_page
        по тому же API — ровно то, что SKILL.md запрещает делать после 429.

        `id` у узла navtree по OpenAPI необязателен (required: slug, title,
        type, has_children), поэтому узел без id связывается с костяком по
        slug — иначе корректный ответ API снова давал бы O(N) get_page.
        """
        need = set()  # предки непокрытых страниц — только туда и спускаемся
        slug2id = {}
        for p in missing:
            pslug = p["slug"].strip("/")
            slug2id[pslug] = p["id"]
            parts = pslug.split("/")
            for i in range(1, len(parts)):
                need.add("/".join(parts[:i]))

        def kids_of(parent):
            try:
                return self._navtree_children(parent)
            except WikiAPIError as e:
                if e.status not in (403, 404):
                    raise
                return []

        frontier, seen = [base_slug], {base_slug}
        with ThreadPoolExecutor(max_workers=self._TITLE_POOL) as ex:
            while frontier:
                nxt = []
                # Широкий уровень (сотни sibling-ветвей) обходим волнами:
                # иначе ex.map отправит их все до того, как станет видна
                # ошибка первой, и исчерпанный 429 обернётся сотнями лишних
                # запросов вместо fail-fast.
                for i in range(0, len(frontier), self._TITLE_BATCH):
                    wave = frontier[i:i + self._TITLE_BATCH]
                    for kids in ex.map(kids_of, wave):
                        for k in kids:
                            kslug = (k.get("slug") or "").strip("/")
                            kid = k.get("id")
                            if kid is None:
                                kid = slug2id.get(kslug)
                            if kid is not None:
                                id2title.setdefault(kid, k.get("title"))
                            if (
                                k.get("has_children")
                                and kslug in need
                                and kslug not in seen
                            ):
                                seen.add(kslug)
                                nxt.append(kslug)
                frontier = nxt

    def descendants_titled(self, page_id, slug, max_depth=10, spine=None):
        """Полный список потомков С заголовками: [{id, slug, title}, ...].

        Костяк всегда из /descendants (он полный). title — слоями, от
        дешёвого к дорогому: /tree одним вызовом (на больших разделах
        обрезается, page_limit_exceeded, ~400 узлов) → батчи navtree по
        ветвям с непокрытыми страницами → get_page в пуле потоков для
        единичных остатков. Раздел на ~10k страниц перечисляется за минуты,
        а не за часы построчного добора (SHARINGML-594). Страница,
        недоступная на доборе (403/404), остаётся в списке с title=None —
        одна удалённая страница не роняет всё перечисление. Сбой сервиса
        (5xx/сеть/429 после повторов) пробрасывается на любом слое, а не
        маскируется под «нет title»; недоступная ветвь navtree (403/404) не
        фатальна — её страницы просто уходят в последний слой.

        spine — уже полученный descendants(page_id): костяк на 12 тыс.
        страниц идёт ~минуту, и оценка размера перед обходом (SKILL.md,
        чтение раздела целиком) иначе выкачивала бы его второй раз. Список
        должен быть именно этого page_id — своей проверки тут нет.
        """
        # url_to_slug идемпотентен, поэтому принимает и URL (tree() ждёт
        # slug), и уже канонический slug — второго percent-декодирования,
        # которое увело бы «%2F» в другой раздел, здесь не будет.
        slug = self.url_to_slug(slug)
        spine = self.descendants(page_id) if spine is None else list(spine)
        id2title = {}

        def walk(node):
            if node.get("id") is not None:
                id2title[node["id"]] = node.get("title")
            for ch in node.get("children") or []:
                walk(ch)

        walk(self.tree(slug, max_depth)["root"])

        missing = [p for p in spine if p["id"] not in id2title]
        if missing:
            self._harvest_titles_via_navtree(slug, missing, id2title)
        missing = [p for p in spine if p["id"] not in id2title]
        if missing:

            def fetch_title(p):
                try:
                    return self.get_page(page_id=p["id"]).get("title")
                except WikiAPIError as e:
                    # Только «страницы нет / нет прав» — ожидаемый исход:
                    # slug остаётся в списке, title=None. 429/5xx/сеть —
                    # сбой сервиса, глотать его нельзя: список выглядел бы
                    # полным, а по нему предписано судить об отсутствии
                    # страницы (см. SKILL.md, чтение кластера целиком).
                    if e.status in (403, 404):
                        return None
                    raise

            with ThreadPoolExecutor(max_workers=self._TITLE_POOL) as ex:
                for i in range(0, len(missing), self._TITLE_BATCH):
                    batch = missing[i:i + self._TITLE_BATCH]
                    for p, title in zip(batch, ex.map(fetch_title, batch)):
                        id2title[p["id"]] = title

        return [
            {"id": p["id"], "slug": p["slug"], "title": id2title.get(p["id"])}
            for p in spine
        ]

    def autocomplete(self, slug, limit=100, privilege=None, with_grids=None):
        """Автодополнение по slug-префиксу (обязательный параметр — slug).

        Возвращает список {id, slug, title, page_type, grids}.

        limit по умолчанию — потолок эндпоинта (100), а не дефолтные 10 API:
        по этой выдаче предписано судить о наличии страницы, и молча
        обрезанный список выглядел бы полным. Полным он всё равно не
        становится: курсора у эндпоинта нет, поэтому ровно limit элементов
        означает «список усечён» — отсутствие страницы этим не доказывается.
        """
        q = [f"slug={urllib.parse.quote(slug)}"]
        if limit is not None:
            q.append(f"limit={limit}")
        if privilege is not None:
            q.append(f"privilege={privilege}")
        if with_grids is not None:
            q.append(f"with_grids={'true' if with_grids else 'false'}")
        resp = self._request("/api/v2/public/pages/autocomplete?" + "&".join(q))
        return resp.get("results", [])

    def backlinks(self, page_id):
        """Полный список ссылающихся страниц. Элемент — {id, slug}."""
        return self._paginate(f"/api/v2/public/pages/{page_id}/backlinks")

    def revisions(self, page_id):
        """Полный список ревизий. Элемент: {id, author, created_at,
        page_type, revision_draft}; id — числовой id ревизии.

        ВАЖНО: порядок элементов API не гарантирован — НЕ полагайся на
        revisions()[-1] как на «последнюю». Для N последних —
        latest_revisions().
        """
        return self._paginate(f"/api/v2/public/pages/{page_id}/revisions")

    def latest_revisions(self, page_id, n=2):
        """N самых свежих ревизий, гарантированно отсортированы по id
        убыв. (latest_revisions(page_id)[0] — самая свежая). Снимает
        ловушку негарантированного порядка revisions()."""
        revs = sorted(self.revisions(page_id), key=lambda r: r["id"], reverse=True)
        return revs[:n]

    def diff(self, page_id, revision_a, revision_b):
        """Diff между ревизиями (целые id ревизий). Форма ответа двухуровневая:

        title_diff — список строк, строка — список сегментов [op, text].
        content — {"diff": [<строка>, ...], "type": "wysiwyg"}, где строка —
        список сегментов [op, text]; op: "=" без изменений, "+" добавлено,
        "-" удалено. Готовый текстовый рендер — diff_text().
        """
        return self._request(
            f"/api/v2/public/pages/{page_id}/revisions/diff"
            f"?revision_a={revision_a}&revision_b={revision_b}"
        )

    @staticmethod
    def _render_lines(lines):
        out = []
        for line in lines:
            seg = "".join(t if op == "=" else f"[{op}{t}]" for op, t in line)
            out.append(seg)
        return "\n".join(out)

    def diff_text(self, page_id, revision_a, revision_b):
        """Читаемый текст изменений между ревизиями.

        Возвращает {"title": <строка>, "content": <строка>}, где удалённое/
        добавленное обёрнуто маркерами [-...]/[+...]. Снимает с вызывающего
        разбор двухуровневой структуры diff().
        """
        d = self.diff(page_id, revision_a, revision_b)
        content = d.get("content") or {}
        return {
            "title": self._render_lines(d.get("title_diff") or []),
            # A grid diff is structured cell/column data, not text segments.
            # Preserve the structure instead of unpacking it as [op, text].
            "content": (
                json.dumps(content, ensure_ascii=False, indent=2)
                if content.get("type") == "grid"
                else self._render_lines(content.get("diff") or [])
            ),
        }

    def comments(self, page_id):
        """Полный список комментариев.

        Элемент: {id, body, inline_text, parent_id, author, thread_id,
        created_at, is_deleted, resolve_status, reactions}.
        """
        return self._paginate(f"/api/v2/public/pages/{page_id}/comments")

    def comment_thread(self, page_id, comment_id):
        """Полный список ОТВЕТОВ комментария; сам корень в выдачу не входит.

        У комментария без ответов список пустой — это не «тред не найден».
        У корневого комментария thread_id=None, поэтому для reply через
        add_comment thread_id подставляй сам: он равен id корня.
        """
        return self._paginate(
            f"/api/v2/public/pages/{page_id}/comments/{comment_id}/thread"
        )

    def attachments(self, page_id):
        """Полный список вложений (эндпоинт пагинированный, отдаёт по 25).

        Элемент: {id, name, download_url, size, mimetype, ...}.
        У WYSIWYG-картинок name бывает обезличенным (image.png) — надёжное
        имя файла берётся из хвоста download_url. size — строка в
        МЕГАБАЙТАХ ('0.25' = 262 302 байта), не в байтах.

        Без пагинации страница с 26+ файлами отдавала бы первые 25 как весь
        список — миграция (references/page_migration.md) чистит и перезаливает
        вложения по нему, и остаток молча оставался бы на исходной странице.
        """
        return self._paginate(f"/api/v2/public/pages/{page_id}/attachments")

    def me(self):
        """Текущий пользователь. Логин — me['username'] (поля login нет),
        uid — me['identity']['uid'] (вложенный). display-имени в /me нет."""
        return self._request("/api/v2/public/me")

    # --- якоря заголовков ---------------------------------------------
    #
    # Wiki рендерит Markdown через @diplodoc/transform, который для
    # заголовков без явного {#anchor} зовёт slugify(title,
    # {lower: true, remove: /[^\w\s$_\-,;=/]+/g}) из пакета simov/slugify.
    # Воспроизводим алгоритм один-в-один — иначе сгенерированный TOC
    # ссылается на якоря, которых на странице нет.

    # Полная таблица simov/slugify (config/charmap.json, 641 entry) лежит
    # рядом в wiki_anchor_charmap.json — храним отдельным файлом, чтобы
    # клиент не раздулся на 700 строк и таблицу легко было обновлять при
    # выходе новых версий simov. Загружается лениво.
    _CHARMAP = None
    _REPLACEMENT = "-"

    @classmethod
    def _charmap(cls):
        if cls._CHARMAP is None:
            path = os.path.join(os.path.dirname(__file__), "wiki_anchor_charmap.json")
            with open(path, "r", encoding="utf-8") as f:
                cls._CHARMAP = json.load(f)
        return cls._CHARMAP

    # whitelist для шага remove. \w в JS — это [A-Za-z0-9_] (ASCII),
    # так что Unicode-флаг здесь НЕ нужен — иначе кириллица, не
    # покрытая charmap, прошла бы дальше, а у Wiki нет.
    _REMOVE_RE = re.compile(r"[^A-Za-z0-9_\s$\-,;=/]+")

    @classmethod
    def _wiki_anchor(cls, title):
        """Якорь заголовка в схеме Yandex Wiki (Diplodoc YFM).

        Воспроизводит @diplodoc/transform: slugify(title, {lower: true,
        remove: /[^\\w\\s$_\\-,;=/]+/g}) из пакета simov/slugify.
        Проверено эмпирически — см. scripts/check_anchor_parity.py."""
        charmap = cls._charmap()
        # 1. Charmap посимвольно. Если результат равен replacement ('-'),
        #    превращаем в пробел: simov делает так, чтобы соседние спецы
        #    схлопнулись через \s+ -> replacement в один дефис.
        buf = []
        for ch in title:
            mapped = charmap.get(ch, ch)
            buf.append(" " if mapped == cls._REPLACEMENT else mapped)
        s = "".join(buf)
        # 2. Remove: убираем всё, чего нет в whitelist (em-dash, точки,
        #    скобки, восклицания, экзотика). Кириллица сюда не доходит —
        #    либо отработана charmap, либо удалится тут.
        s = cls._REMOVE_RE.sub("", s)
        # 3. Trim, 4. \s+ -> '-', 5. lower.
        s = re.sub(r"\s+", cls._REPLACEMENT, s.strip()).lower()
        return s

    @classmethod
    def heading_anchor(cls, title, translit=None):
        """Якорь заголовка для Yandex Wiki. Параметр translit оставлен
        для обратной совместимости — игнорируется, схема всегда одна
        (Diplodoc translit). См. references/wiki-anchors.md."""
        return cls._wiki_anchor(title)

    # Wiki рендерит raw-HTML блоки (`::: html`) как есть: Diplodoc-якоря для
    # таких заголовков не генерируются, работают только явные id-атрибуты.
    _HTML_HEADING_RE = re.compile(r"<h([1-6])\b([^>]*)>(.*?)</h\1\s*>", re.S | re.I)
    _HTML_ID_RE = re.compile(r"<[a-zA-Z][\w-]*\b[^>]*?\bid\s*=\s*[\"']([^\"']+)[\"']")
    _ATTR_ID_RE = re.compile(r"\bid\s*=\s*[\"']([^\"']+)[\"']")
    _TAG_STRIP_RE = re.compile(r"<[^>]+>")
    _TAG_RE = re.compile(r"<(/?)([a-zA-Z][\w-]*)((?:[^>\"']|\"[^\"]*\"|'[^']*')*)>")
    _VOID_TAGS = frozenset(
        "area base br col embed hr img input link meta source track wbr".split()
    )

    # Diplodoc использует markdown-it. Тот же CommonMark-блочный разбор нужен,
    # чтобы корректно учитывать fence внутри list/blockquote-контейнеров и не
    # принимать literal ``` внутри raw-HTML block за начало кода. Parser
    # создаётся лениво: без пакета CRUD-клиент остаётся работоспособным.
    _MARKDOWN_BLOCK_PARSER = None
    _MARKDOWN_PARSER_UNAVAILABLE = False
    _CODE_BLOCK_TYPES = frozenset({"fence", "code_block"})
    _RAW_HTML_OPEN_RE = re.compile(r"^ {0,3}:::[ \t]+html(?:[ \t].*)?$")
    _RAW_HTML_CLOSE_RE = re.compile(r"^ {0,3}:::[ \t]*$")
    _ATX_HEADING_RE = re.compile(
        r"^ {0,3}(#{1,6})(?:[ \t]+(.*?))?[ \t]*\r?$", re.M
    )

    def _enclosing_id(self, content, pos):
        """id ближайшего объемлющего тега для позиции pos (стек открытых
        тегов по упрощённому разбору; вёрстка Wiki не всегда идеальна,
        поэтому это эвристика для наследования якоря заголовком)."""
        stack = []
        for m in self._TAG_RE.finditer(content, 0, pos):
            closing, tag, attrs = m.group(1), m.group(2).lower(), m.group(3)
            if tag in self._VOID_TAGS or attrs.rstrip().endswith("/"):
                continue
            if closing:
                for i in range(len(stack) - 1, -1, -1):
                    if stack[i][0] == tag:
                        del stack[i:]
                        break
            else:
                mid = self._ATTR_ID_RE.search(attrs)
                stack.append((tag, mid.group(1) if mid else None))
        for _, id_ in reversed(stack):
            if id_:
                return id_
        return None

    @classmethod
    def _markdown_block_tokens(cls, content):
        if cls._MARKDOWN_PARSER_UNAVAILABLE:
            return ()
        if cls._MARKDOWN_BLOCK_PARSER is None:
            try:
                cls._MARKDOWN_BLOCK_PARSER = _new_markdown_block_parser()
            except ImportError:
                cls._MARKDOWN_PARSER_UNAVAILABLE = True
                warnings.warn(
                    "markdown-it-py не установлен: headings()/build_toc()/"
                    "resolve_anchor() работают без маскирования CommonMark "
                    "code blocks; переустановите зависимости stefania-wiki",
                    RuntimeWarning,
                    stacklevel=2,
                )
                return ()
        return cls._MARKDOWN_BLOCK_PARSER.parse(content)

    @staticmethod
    def _parser_lines(content):
        """Разбить текст ровно по newline-правилу markdown-it.

        `str.splitlines()` дополнительно режет по form feed, NEL и Unicode
        line separators, из-за чего token.map начинает указывать не на те
        строки. Сохраняем исходные CR/LF и длину текста побайтно.
        """
        parts = re.split(r"(\r\n?|\n)", content)
        return ["".join(parts[i : i + 2]) for i in range(0, len(parts), 2)]

    @classmethod
    def _container_blocking_ranges(cls, tokens):
        """Диапазоны строк, внутри которых `::: html` не открывает
        Diplodoc-контейнер: fenced/indented code и CommonMark `html_block`.
        Внутри raw-HTML блока (`<div>…</div>`, `<pre>…`) строка `::: html` —
        литеральный текст, а не директива, поэтому её нельзя считать OPEN."""
        ranges = [
            tuple(token.map)
            for token in tokens
            if token.type in cls._CODE_BLOCK_TYPES or token.type == "html_block"
        ]
        ranges.sort()
        return ranges

    @classmethod
    def _raw_html_line_ranges(cls, lines, blocking_ranges):
        """Диапазоны Diplodoc `::: html` в координатах строк parser-а.

        blocking_ranges — отсортированные по началу непересекающиеся
        диапазоны (code blocks + html_block), внутри которых `::: html`
        игнорируется (см. `_container_blocking_ranges`)."""
        ranges = []
        start = None
        block_iter = iter(blocking_ranges)
        current_block = next(block_iter, None)
        for i, line in enumerate(lines):
            body = line.rstrip("\r\n")
            if start is None:
                while current_block is not None and i >= current_block[1]:
                    current_block = next(block_iter, None)
                inside_block = (
                    current_block is not None
                    and current_block[0] <= i < current_block[1]
                )
                if not inside_block and cls._RAW_HTML_OPEN_RE.fullmatch(body):
                    start = i
            elif cls._RAW_HTML_CLOSE_RE.fullmatch(body):
                ranges.append((start, i + 1))
                start = None
        if start is not None:
            ranges.append((start, len(lines)))
        return ranges

    @staticmethod
    def _mask_line_ranges(lines, ranges):
        for start, end in ranges:
            for i in range(start, end):
                lines[i] = re.sub(r"[^\r\n]", " ", lines[i])

    @classmethod
    def _mask_blocks(cls, content, tokens, mask_html_blocks=False):
        if cls._MARKDOWN_PARSER_UNAVAILABLE:
            # Без парсера границы code blocks и `::: html` неизвестны —
            # маскировать нечем. Возвращаем контент как есть (деградация до
            # разбора по regex, как обещает warning), а не съедаем заголовки
            # мнимым `::: html`-контейнером до конца страницы.
            return content
        lines = cls._parser_lines(content)
        raw_html_ranges = cls._raw_html_line_ranges(
            lines, cls._container_blocking_ranges(tokens)
        )

        # CommonMark не знает границ `::: html`: fence/html_block, начавшийся
        # внутри контейнера, может поглотить Markdown после закрывающего `:::`.
        # Маскируем custom-container и разбираем оставшийся Markdown повторно.
        # Повторяем до fixed point: ошибочный fence из первого контейнера мог
        # скрыть от начального parse следующий `::: html`. Диапазоны каждый
        # раз заменяем полностью: после маски предыдущего контейнера другой
        # `::: html` может, наоборот, оказаться внутри настоящего fence.
        parser_tokens = tokens
        parsed_states = {}
        while True:
            state = tuple(raw_html_ranges)
            if state in parsed_states:
                # Патологический цикл неоднозначных границ: используем parse,
                # соответствующий текущему набору ranges, и завершаемся.
                parser_tokens = parsed_states[state]
                break
            if raw_html_ranges:
                parser_lines = lines.copy()
                cls._mask_line_ranges(parser_lines, raw_html_ranges)
                parser_tokens = cls._markdown_block_tokens("".join(parser_lines))
            else:
                parser_tokens = tokens
            parsed_states[state] = parser_tokens
            discovered_ranges = cls._raw_html_line_ranges(
                lines, cls._container_blocking_ranges(parser_tokens)
            )
            if discovered_ranges == raw_html_ranges:
                break
            raw_html_ranges = discovered_ranges

        code_ranges = [
            tuple(token.map)
            for token in parser_tokens
            if token.type in cls._CODE_BLOCK_TYPES
        ]
        ranges = list(raw_html_ranges) if mask_html_blocks else []

        for start, end in code_ranges:
            ranges.append((start, end))
        if mask_html_blocks:
            ranges.extend(
                tuple(token.map)
                for token in parser_tokens
                if token.type == "html_block"
            )

        cls._mask_line_ranges(lines, ranges)
        return "".join(lines)

    @classmethod
    def _mask_code_blocks(cls, content):
        """Затереть code blocks, сохранив офсеты и переводы строк.

        Fenced/indented blocks берутся из CommonMark parser-а. Диапазоны
        `::: html` остаются нетронутыми: их содержимое Diplodoc рендерит как
        raw HTML, даже если внутри встречаются строки с backticks.
        """
        tokens = cls._markdown_block_tokens(content)
        return cls._mask_blocks(content, tokens)

    def headings(self, page_id=None, slug=None, content=None):
        """Заголовки страницы с готовым к подстановке якорем:
        [{level, text, anchor, explicit, anchor_en, html}].

        anchor — реальный id, который Wiki поставит при рендере: для
        Markdown-заголовков — Diplodoc-схема (translit + лат-пунктуация),
        для HTML-заголовков (`<h2 id=...>` внутри `::: html`) — их явный
        id-атрибут; HTML-заголовки без id якоря при рендере не получают и
        в список не попадают. explicit=True, если якорь задан явно
        ({#custom-id} или id-атрибут); html=True — заголовок из raw-HTML.
        anchor_en — алиас на anchor для обратной совместимости.

        Содержимое code blocks заголовками и HTML-якорями не считается — как
        и при рендере Diplodoc. Markdown-заголовки внутри raw HTML blocks тоже
        игнорируются, а явные HTML id сохраняются."""
        if content is None:
            page = self.get_page(slug=slug, page_id=page_id, fields=["content"])
            content = page.get("content", "")
        found, seen = [], {}

        def uniq(base):
            # Diplodoc для дубликатов прибавляет порядковый номер БЕЗ
            # дефиса: 'item', 'item1', 'item2', ...
            if base not in seen:
                seen[base] = 1
                return base
            n = seen[base]
            seen[base] = n + 1
            return f"{base}{n}"

        tokens = self._markdown_block_tokens(content)
        visible_content = self._mask_blocks(content, tokens)
        markdown_content = self._mask_blocks(
            content, tokens, mask_html_blocks=True
        )
        # Python re.MULTILINE распознаёт началом строки только позицию после
        # LF, тогда как CommonMark нормализует и одиночный CR. Замена
        # сохраняет длину, поэтому порядок с HTML-heading остаётся точным.
        markdown_content = re.sub(r"\r(?!\n)", "\n", markdown_content)
        for m in self._ATX_HEADING_RE.finditer(markdown_content):
            raw = (m.group(2) or "").strip()
            explicit = re.search(r"\{#([^}]+)\}\s*$", raw)
            if explicit:
                # Явный якорь у заголовка — литеральный, текст
                # показываем без {#...}.
                base = explicit.group(1).strip()
                text = raw[: explicit.start()].strip()
                is_explicit = True
            else:
                base = self._wiki_anchor(raw)
                text = raw
                is_explicit = False
            if not base:
                # Пустому заголовку (`#` без текста, `{#}`) Diplodoc id при
                # рендере не проставляет — навигационного якоря нет, в список
                # (и в build_toc/resolve_anchor через него) такой заголовок
                # не попадает, иначе получилась бы мёртвая ссылка `[](#)`.
                continue
            anchor = uniq(base)
            found.append(
                (
                    m.start(),
                    {
                        "level": len(m.group(1)),
                        "text": text,
                        "anchor": anchor,
                        "anchor_en": anchor,  # алиас, оставлен для совместимости
                        "explicit": is_explicit,
                        "html": False,
                        "inherited": False,
                    },
                )
            )
        for m in self._HTML_HEADING_RE.finditer(visible_content):
            mid = self._ATTR_ID_RE.search(m.group(2))
            inherited = False
            if mid:
                anchor = mid.group(1)
            else:
                # Частая вёрстка: id стоит на секции-контейнере, а не на
                # самом <h2> — навигационный якорь заголовка наследуется
                # от ближайшего объемлющего тега с id.
                anchor = self._enclosing_id(visible_content, m.start())
                inherited = anchor is not None
                if anchor is None:
                    continue
            text = self._TAG_STRIP_RE.sub("", m.group(3)).strip()
            found.append(
                (
                    m.start(),
                    {
                        "level": int(m.group(1)),
                        "text": text,
                        "anchor": anchor,
                        "anchor_en": anchor,
                        "explicit": True,
                        "html": True,
                        "inherited": inherited,
                    },
                )
            )
        return [h for _, h in sorted(found, key=lambda x: x[0])]

    def resolve_anchor(self, anchor, page_id=None, slug=None, content=None):
        """Проверить #-якорь против якорей страницы.

        anchor — '#foo', 'foo' или URL '.../slug/#foo'. Валидными считаются
        якоря заголовков (Markdown → Diplodoc-схема, HTML → id-атрибут) и
        любые явные id-атрибуты элементов в raw-HTML контенте (`<section
        id=...>` — браузер скроллит к любому id). Возвращает {valid,
        heading, anchor, suggestions}; scheme — 'translit' для заголовка,
        'html-id' для прочего элемента с id. Для совместимости — поля
        anchor_cyrillic=anchor_translit=anchor. Если ссылка битая — в
        suggestions заголовки/элементы с правильными якорями."""
        a = anchor.split("#", 1)[-1].strip().strip("/")
        if content is None:
            page = self.get_page(slug=slug, page_id=page_id, fields=["content"])
            content = page.get("content", "")
        hs = self.headings(content=content)
        for h in hs:
            if a == h["anchor"]:
                return {
                    "valid": True,
                    "scheme": "translit",
                    "heading": h["text"],
                    "anchor": h["anchor"],
                    "anchor_cyrillic": h["anchor"],
                    "anchor_translit": h["anchor"],
                    "suggestions": [],
                }
        visible_content = self._mask_code_blocks(content)
        element_ids = []
        heading_anchors = {h["anchor"] for h in hs}
        for m in self._HTML_ID_RE.finditer(visible_content):
            if m.group(1) not in heading_anchors:
                element_ids.append(m.group(1))
        if a in element_ids:
            return {
                "valid": True,
                "scheme": "html-id",
                "heading": None,
                "anchor": a,
                "anchor_cyrillic": a,
                "anchor_translit": a,
                "suggestions": [],
            }
        prefix = a[:6]
        candidates = [{"heading": h["text"], "anchor": h["anchor"]} for h in hs] + [
            {"heading": None, "anchor": i} for i in element_ids
        ]
        sug = [c for c in candidates if prefix and prefix in c["anchor"]]
        return {
            "valid": False,
            "scheme": None,
            "heading": None,
            "anchor": None,
            "anchor_cyrillic": None,
            "anchor_translit": None,
            "suggestions": sug or candidates,
        }

    def build_toc(
        self,
        page_id=None,
        slug=None,
        content=None,
        levels=(2, 3),
        include_title=False,
    ):
        """Сгенерировать готовый Markdown-блок оглавления страницы.

        Берёт заголовки заданных levels (по умолчанию h2/h3), строит
        список '- [Текст](#anchor)' с отступом по уровню. Якоря —
        ровно те, что Wiki проставит при рендере (Diplodoc-схема).

        Используй этот метод вместо ручной генерации ссылок: при
        ручной сборке легко промахнуться по правилам simov/slugify
        (точки удаляются, не заменяются дефисом; кириллица идёт в
        translit, а не остаётся как есть)."""
        hs = self.headings(page_id=page_id, slug=slug, content=content)
        levels = tuple(levels)
        if not levels:
            return ""
        base = min(levels)
        lines = []
        if include_title:
            lines.append("## Содержание")
            lines.append("")
        for h in hs:
            if h["level"] not in levels:
                continue
            indent = "  " * (h["level"] - base)
            # экранируем закрывающую квадратную скобку в тексте — иначе
            # ломается Markdown-ссылка
            text = h["text"].replace("]", r"\]")
            lines.append(f"{indent}- [{text}](#{h['anchor']})")
        return "\n".join(lines)

    # --- запись --------------------------------------------------------

    def create_page(self, title, slug, page_type="wysiwyg", content=None):
        """Создать страницу. Обязательны title, slug, page_type."""
        data = {"title": title, "slug": slug, "page_type": page_type}
        if content is not None:
            data["content"] = content
        return self._request("/api/v2/public/pages", "POST", data)

    def update_page(self, page_id, **fields):
        """Полная замена полей страницы (title/content/...).

        Для optimistic locking передай revision=<last_revision_id>,
        прочитанный через get_page(..., fields=['last_revision_id']).
        """
        return self._request(f"/api/v2/public/pages/{page_id}", "POST", fields)

    def append_content(self, page_id, content, location=None):
        """Дописать контент. location='top' — в начало (по умолч. в конец).

        API требует один из взаимоисключающих указателей места дописывания:
        `body`, `section` или `anchor` — без них валидация падает с
        VALIDATION_ERROR. Здесь используем `body: {location: top|bottom}`.
        """
        data = {
            "content": content,
            "body": {"location": location or "bottom"},
        }
        return self._request(
            f"/api/v2/public/pages/{page_id}/append_content", "POST", data
        )

    def suggest_slug(self, title, current_slug=None):
        """Сгенерировать slug из заголовка. current_slug — slug раздела-
        родителя, чтобы slug предложился внутри него, а не в корне."""
        q = f"?title={urllib.parse.quote(title)}"
        if current_slug is not None:
            q += f"&current_slug={urllib.parse.quote(current_slug)}"
        return self._request(f"/api/v2/public/pages/suggest_slug{q}")

    def move(self, operations, dry_run=False, wait=False):
        """Переместить страницы. operations=[{source, target}, ...].

        Асинхронно. wait=False (по умолч.) — вернуть id операции;
        wait=True — дождаться и вернуть финальный статус.

        dry_run=True — только проверка, и wait при ней игнорируется: сервер
        отвечает 200 с id операции, но самой операции не создаёт, а опрос
        статуса даёт 404. Клиент раньше на этом падал — «проверь, потом
        перенеси» выглядело как «перенос невозможен». Ошибки валидации
        приходят синхронно, как WikiAPIError: CLUSTER_NOT_EXISTS,
        OVERRIDE_ATTEMPT (на целевом slug уже есть страница),
        MOVE_INTO_ITSELF. Прошёл dry_run без исключения — перенос пройдёт.
        """
        path = "/api/v2/public/pages/move"
        if dry_run:
            path += "?dry_run=true"
        resp = self._request(path, "POST", {"operations": operations})
        op_id = resp["operation"]["id"]
        if dry_run or not wait:
            return op_id
        return self._wait_operation("move", op_id)

    def clone(self, page_id, target, title=None, poll=True):
        """Клонировать страницу (асинхронно).

        poll=True — дождаться и вернуть {id, slug} новой страницы.
        Статусы операции: scheduled, in_progress, success, failed.
        """
        data = {"target": target}
        if title is not None:
            data["title"] = title
        resp = self._request(f"/api/v2/public/pages/{page_id}/clone", "POST", data)
        op_id = resp["operation"]["id"]
        if not poll:
            return op_id
        return self._wait_operation("clone", op_id).get("result", {}).get("page")

    def delete_page(self, page_id, recursive=False):
        """Удалить страницу. recursive=True — снести и поддерево.

        Без recursive страница с детьми не удаляется (HAS_CHILDREN). Откат —
        только recovery_tokens()/recover_page() и только для своих удалений,
        поэтому перед вызовом показывай состав поддерева (descendants).

        Ответ несёт recovery_token — один на всё удалённое поддерево, окно
        восстановления ~30 дней. Сохрани его: recover_page(token) поднимает
        поддерево целиком с теми же page_id, это undelete, а не пересоздание.
        """
        path = f"/api/v2/public/pages/{page_id}"
        if recursive:
            path += "?recursive=true"
        return self._request(path, "DELETE")

    # --- комментарии ---------------------------------------------------

    def add_comment(
        self, page_id, body, inline_text=None, parent_id=None, thread_id=None
    ):
        """Создать комментарий. inline_text должен точно совпадать с текстом
        страницы (включая экранирование \\_).

        Ответ в тред (reply) требует ОБА параметра: parent_id — id
        комментария, на который отвечаешь, и thread_id — id корневого
        комментария треда (при ответе на сам корень thread_id == parent_id).
        Без thread_id API создаёт reply как самостоятельный комментарий, не
        привязанный к треду, — comment_thread() его не вернёт."""
        data = {"body": body}
        if inline_text is not None:
            data["inline_text"] = inline_text
        if parent_id is not None:
            data["parent_id"] = parent_id
        if thread_id is not None:
            data["thread_id"] = thread_id
        return self._request(f"/api/v2/public/pages/{page_id}/comments", "POST", data)

    def edit_comment(self, page_id, comment_id, body):
        return self._request(
            f"/api/v2/public/pages/{page_id}/comments/{comment_id}",
            "POST",
            {"body": body},
        )

    def delete_comment(self, page_id, comment_id):
        return self._request(
            f"/api/v2/public/pages/{page_id}/comments/{comment_id}",
            "DELETE",
        )

    def set_comment_status(self, page_id, comment_id, status):
        """status: 'resolved' | 'unresolved'."""
        return self._request(
            f"/api/v2/public/pages/{page_id}/comments/{comment_id}/set_status",
            "POST",
            {"new_status": status},
        )

    # --- вложения ------------------------------------------------------

    def upload_attachment(self, page_id, filename, file_data):
        """Загрузить файл (флоу upload_sessions).

        Возвращает ответ attach-эндпоинта — это КОЛЛЕКЦИЯ, а не объект
        вложения: {"results": [{id, name, download_url, ...}]}. Прямое
        res["id"] даёт None и следующий вызов по нему падает 404; бери
        res["results"][0].

        Шлёт файл одним куском (part_number=1) — для больших файлов API
        предполагает нарезку на части >= 5 МБ с инкрементом part_number,
        и такую загрузку этот метод не делает: понадобится — собирай цикл
        по upload_part поверх _request/этого же upload_url.
        """
        sess = self._request(
            "/api/v2/public/upload_sessions",
            "POST",
            {
                "target": "attachment",
                "file_name": filename,
                "file_size": len(file_data),
            },
        )
        sid = sess["session_id"]
        up_url = (
            f"{BASE_URL}/api/v2/public/upload_sessions/{sid}"
            f"/upload_part?part_number=1"
        )
        req = urllib.request.Request(
            up_url,
            data=file_data,
            headers={
                "Authorization": f"OAuth {self._token}",
                "Content-Type": "application/octet-stream",
            },
            method="PUT",
        )
        # Сырой PUT тела мимо _request (там JSON) — ошибки приводим к тому
        # же контракту: наружу WikiAPIError, а не голый HTTPError/OSError.
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                resp.read()
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            raise WikiAPIError(e.code, "UPLOAD_FAILED", body[:500]) from None
        except (OSError, http.client.HTTPException) as e:
            raise WikiAPIError(0, "NETWORK", str(getattr(e, "reason", e))) from None
        self._request(f"/api/v2/public/upload_sessions/{sid}/finish", "POST")
        return self._request(
            f"/api/v2/public/pages/{page_id}/attachments",
            "POST",
            {"upload_sessions": [sid]},
        )

    def download_attachment(self, page_id, file_id):
        """Бинарное содержимое вложения.

        На несуществующий file_id эндпоинт отдаёт не JSON-ошибку, а картинку-
        заглушку (GIF) с кодом 200 — отличить её от настоящего файла можно
        только по содержимому, WikiAPIError тут не будет. Список актуальных
        id — attachments(); удалённый файл при этом какое-то время ещё
        скачивается по прямому id, хотя из attachments() уже исчез.
        """
        return self._request(
            f"/api/v2/public/pages/{page_id}/attachments/{file_id}/download",
            raw=True,
        )

    def delete_attachment(self, page_id, file_id):
        return self._request(
            f"/api/v2/public/pages/{page_id}/attachments/{file_id}",
            "DELETE",
        )

    # --- гриды ---------------------------------------------------------

    @staticmethod
    def _grid_page_identity(page_slug=None, page_id=None):
        """Собрать identity родительской страницы; нужен ровно один slug/id."""
        if (page_slug is None) == (page_id is None):
            raise ValueError("Передай ровно один из page_slug или page_id")
        return {"slug": page_slug} if page_slug is not None else {"id": page_id}

    def create_grid_page(self, title, slug):
        """Создать grid-страницу с явным slug и её основной UI-грид.

        Pages API автоматически создаёт основной UI-грид. Его числовой ID
        равен id возвращённой страницы; отдельный вызов create_grid() после
        этого НЕ нужен — он создал бы ещё одну дочернюю grid-страницу.
        """
        return self.create_page(title, slug, page_type="grid")

    def create_grid(self, title, page_slug=None, page_id=None):
        """Создать дочернюю grid-страницу под указанным родителем.

        Compat API сам назначает дочерней странице slug. Возвращаемый числовой
        id одновременно является page_id и ID её основного UI-грида. Если
        нужен заранее заданный slug, используй create_grid_page().
        Для отдельного UUID-ресурса используй create_grid_resource().
        """
        page = self._grid_page_identity(page_slug=page_slug, page_id=page_id)
        return self._request(
            "/api/v2/public/grids/compat",
            "POST",
            {"title": title, "page": page},
        )

    def create_grid_resource(self, title, page_slug=None, page_id=None):
        """Создать отдельный UUID-grid resource.

        Такой ресурс виден через page_grids() и CRUD `/grids/<uuid>`, но не
        является основным содержимым grid-страницы и сам по себе не рендерится
        в Wiki UI. Для grid-страницы с явным slug используй create_grid_page().
        """
        page = self._grid_page_identity(page_slug=page_slug, page_id=page_id)
        return self._request(
            "/api/v2/public/grids",
            "POST",
            {"title": title, "page": page},
        )

    def get_grid(self, grid_id):
        """Грид по id: int → UI-грид страницы, UUID → отдельный ресурс.

        Ответ: {id, title, page:{id,slug},
        structure.columns (описание колонок), rows:[{id,row:[значения]}],
        revision}. Порядок значений в row совпадает с structure.columns."""
        return self._request(f"/api/v2/public/grids/{grid_id}")

    def get_page_grid(self, page_id):
        """Основной UI-грид grid-страницы по её числовому page_id."""
        if not isinstance(page_id, int) or isinstance(page_id, bool):
            raise ValueError("page_id UI-грида должен быть целым числом")
        return self.get_grid(page_id)

    def add_grid_rows(self, grid_id, rows, revision=None):
        """Добавить строки: int id → UI-грид, UUID → отдельный ресурс.

        rows=[{col_slug: value, ...}]. Значение зависит от типа колонки:
        string->str, number->int/float, date->ISO-строка ('2026-06-01'),
        checkbox->bool, select->СПИСОК строк даже при multiple:false (['opt']),
        staff->список логинов, ticket->ключ тикета. Неверный формат (часто —
        select не списком) дает VALIDATION_ERROR."""
        data = {"rows": rows}
        if revision is not None:
            data["revision"] = revision
        return self._request(f"/api/v2/public/grids/{grid_id}/rows", "POST", data)

    def delete_grid_rows(self, grid_id, row_ids):
        return self._request(
            f"/api/v2/public/grids/{grid_id}/rows",
            "DELETE",
            {"row_ids": row_ids},
        )

    def update_grid_cells(self, grid_id, cells):
        """Обновить ячейки: int id → UI-грид, UUID → отдельный ресурс.

        cells=[{row_id:int, column_slug:str, value:any}, ...].

        value — по типу колонки (как в add_grid_rows): date->ISO-строка,
        checkbox->bool, select->список строк даже при multiple:false."""
        return self._request(
            f"/api/v2/public/grids/{grid_id}/cells",
            "POST",
            {"cells": cells},
        )

    def add_grid_columns(self, grid_id, columns):
        """Добавить колонки: int id → UI-грид, UUID → отдельный ресурс.

        Каждая колонка обязана иметь slug, title, type, required.

        type: string|number|date|select|staff|checkbox|ticket|ticket_field.

        У select-колонки варианты задаются полем select_options (список
        строк) плюс multiple (bool): {"slug": "status", "title": "Статус",
        "type": "select", "required": False, "select_options": ["Активен",
        "В отпуске"], "multiple": False}. Имя поля именно такое — options
        или values дают VALIDATION_ERROR. Значение такой колонки в строке
        всё равно список, даже при multiple=False (см. add_grid_rows).
        """
        return self._request(
            f"/api/v2/public/grids/{grid_id}/columns",
            "POST",
            {"columns": columns},
        )

    def delete_grid_columns(self, grid_id, column_slugs):
        return self._request(
            f"/api/v2/public/grids/{grid_id}/columns",
            "DELETE",
            {"column_slugs": column_slugs},
        )

    def export_grid(self, grid_id, file_format="csv"):
        """file_format: csv | xls | docx (xlsx НЕ поддерживается)."""
        return self._request(
            f"/api/v2/public/grids/{grid_id}/export/{file_format}",
            raw=True,
        )

    def clone_grid(self, grid_id, target, title=None, with_data=True, poll=True):
        """Клонировать грид: UUID -> clone_inline_grid, UI -> clone_grid."""
        data = {"target": target, "with_data": with_data}
        if title is not None:
            data["title"] = title
        resp = self._request(f"/api/v2/public/grids/{grid_id}/clone", "POST", data)
        operation = resp["operation"]
        op_id = operation["id"]
        if not poll:
            return op_id
        # Wiki использует разные типы операций для UUID-ресурса и UI-грида.
        # Старые ответы/фейки без type сохраняют прежний контракт clone_grid.
        kind = operation.get("type", "clone_grid")
        if kind not in {"clone_grid", "clone_inline_grid"}:
            raise WikiAPIError(
                0, "UNEXPECTED_OPERATION", "Unknown grid clone operation type"
            )
        return self._wait_operation(kind, op_id).get("result")

    def grid_revisions(self, grid_id):
        """Полный список ревизий UUID-ресурса (эндпоинт отдаёт по 25).

        UI-грид страницы версионируется ревизиями самой страницы — для него
        revisions()/latest_revisions().
        """
        return self._paginate(f"/api/v2/public/grids/{grid_id}/revisions")

    def grid_diff(self, grid_id, revision_a, revision_b):
        """Diff ревизий UUID-ресурса."""
        return self._request(
            f"/api/v2/public/grids/{grid_id}/revisions/diff"
            f"?revision_a={revision_a}&revision_b={revision_b}"
        )

    def page_grids(self, page_id):
        """UUID-grid resources, привязанные к странице.

        Основной UI-грид grid-страницы сюда не входит: у здоровой grid-страницы
        список может быть пустым. Его читай через get_page_grid(page_id).
        """
        return self._paginate(f"/api/v2/public/pages/{page_id}/grids")

    # --- доступ и прочее ----------------------------------------------

    def add_access(self, page_id, role, user_uid=None, group=None, inheritance=None):
        """Выдать доступ. role: reader|editor|extra_editor|author.

        Пользователю — user_uid. Группе — group={'src': 'staff'|'dir'|
        'cloud'|'com', 'id': '<gid>'} (src обязателен).
        """
        data = {"role": role}
        if user_uid is not None:
            data["user"] = {"uid": user_uid}
        if group is not None:
            data["group"] = group
        if inheritance is not None:
            data["inheritance"] = inheritance
        return self._request(f"/api/v2/public/pages/{page_id}/access", "POST", data)

    def inheritable_acl(self, page_id):
        """Наследуемый ACL страницы."""
        return self._request(f"/api/v2/public/pages/{page_id}/inheritable_acl")

    def remove_access(self, page_id, access_id=None):
        """Снять доступ: access_id — конкретный; без него — все."""
        path = f"/api/v2/public/pages/{page_id}/access"
        if access_id is not None:
            path += f"/{access_id}"
        return self._request(path, "DELETE")

    def grant_author_role(self, page_id, user_uid):
        """Назначить автора. Тело обязательно: {'user': {'uid': ...}}."""
        return self._request(
            f"/api/v2/public/pages/{page_id}/grant_author_role",
            "POST",
            {"user": {"uid": user_uid}},
        )

    def change_order(self, page_id, next_to_slug, position=None):
        """Поставить страницу рядом с next_to_slug (обязательный параметр)."""
        path = (
            f"/api/v2/public/pages/{page_id}/change_order"
            f"?next_to_slug={urllib.parse.quote(next_to_slug)}"
        )
        if position is not None:
            path += f"&position={position}"
        return self._request(path, "POST")

    # --- подписки, черновики, прочее ----------------------------------

    def subscribers(self, page_id):
        """Полный список подписчиков страницы.

        Элемент: {id: subscription_id, is_cluster: bool,
                  user: {identity: {uid: str}, ...}}.
        """
        return self._paginate(f"/api/v2/public/pages/{page_id}/subscribers")

    def subscribe(self, page_id, user_uids, is_cluster=True, message=None):
        """Подписать пользователей. is_cluster=True — и на подстраницы."""
        data = {
            "users": [{"uid": u} for u in user_uids],
            "is_cluster": is_cluster,
        }
        if message is not None:
            data["message"] = message
        return self._request(
            f"/api/v2/public/pages/{page_id}/subscribers", "POST", data
        )

    def unsubscribe(self, page_id, subscription_id):
        """Отписать по subscription_id (его id брать из subscribers())."""
        return self._request(
            f"/api/v2/public/pages/{page_id}/subscribers/{subscription_id}",
            "DELETE",
        )

    def create_draft(self, title, content=None, slug=None):
        """Создать черновик (page_type только wysiwyg)."""
        data = {"title": title, "page_type": "wysiwyg"}
        if content is not None:
            data["content"] = content
        if slug is not None:
            data["slug"] = slug
        return self._request("/api/v2/public/pages/drafts", "POST", data)

    def publish_draft(self, draft_id, **fields):
        """Опубликовать черновик (тело обязательно, можно пустое)."""
        return self._request(
            f"/api/v2/public/pages/drafts/{draft_id}/publish", "POST", fields
        )

    def change_to_yfm(self, page_id, content=None):
        """Конвертировать legacy-страницу (page_type='page') в YFM.

        content обязателен для API; если не передан — клиент сам вычитает
        текущий контент страницы (снимает ловушку «конвертнуть = один вызов»).

        Применим только к типу 'page' (старый WOM-формат): на 'wysiwyg' API
        отвечает 400 NOT_IMPLEMENTED, потому что wysiwyg — это уже YFM.
        Отдельного page_type 'yfm' в API нет, конвертировать такую страницу
        не нужно.
        """
        if content is None:
            content = self.get_page(page_id=page_id, fields=["content"]).get(
                "content", ""
            )
        return self._request(
            f"/api/v2/public/pages/{page_id}/change_to_yfm",
            "POST",
            {"content": content},
        )

    def recovery_tokens(self):
        """Полный список токенов восстановления СВОИХ удалённых страниц.

        Эндпоинт фильтрует по token_type (дефолт user_pages) — страница,
        удалённая другим человеком, в эту выдачу не попадёт, и её отсутствие
        не значит, что восстановить нельзя.

        Эндпоинт пагинированный: первая страница выдачи — 50 токенов, и по
        усечённому списку нужный токен выглядел бы отсутствующим.
        """
        return self._paginate("/api/v2/public/recovery_tokens")

    def recover_page(self, token_id):
        """Восстановить удалённую страницу по token_id."""
        return self._request(
            f"/api/v2/public/recovery_tokens/{token_id}/recover", "POST"
        )
