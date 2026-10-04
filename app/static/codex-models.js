const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export function modelOptions(models,selected){
 const rows=models||[],fallback=rows.find(m=>m.is_default);
 let html=`<option value="" ${!selected?'selected':''}>По умолчанию${fallback?' · '+esc(fallback.label):''}</option>`;
 if(selected&&!rows.some(m=>m.model===selected))html+=`<option value="${esc(selected)}" selected disabled>${esc(selected)} · нет в текущем списке</option>`;
 return html+rows.map(m=>`<option value="${esc(m.model)}" ${selected===m.model?'selected':''}>${esc(m.label)}${m.is_default?' · по умолчанию':''}</option>`).join('');
}
