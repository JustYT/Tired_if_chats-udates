export function wikiPersonalUrl(login){
 return /^[a-z][a-z0-9._-]{1,48}$/.test(login||'')
  ? `https://wiki.yandex-team.ru/users/${login}/` : '';
}

export function wikiReceiptUrl(receipt){
 try{
  const url=new URL(receipt);
  if(url.protocol!=='https:'||url.hostname!=='wiki.yandex-team.ru'||url.port||
     url.username||url.password||url.search||url.hash||
     !/^\/users\/[a-z][a-z0-9._-]{1,48}\/(?:[a-zA-Z0-9_-]+\/)*summary-[a-f0-9]{32}\/$/.test(url.pathname))return '';
  return url.href;
 }catch{return '';}
}
