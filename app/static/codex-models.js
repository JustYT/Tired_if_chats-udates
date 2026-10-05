const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export function modelOptions(models,selected){
 const rows=models||[],fallback=rows.find(m=>m.is_default);
 let html=`<option value="" ${!selected?'selected':''}>По умолчанию${fallback?' · '+esc(fallback.label):''}</option>`;
 if(selected&&!rows.some(m=>m.model===selected))html+=`<option value="${esc(selected)}" selected disabled>${esc(selected)} · нет в текущем списке</option>`;
 return html+rows.map(m=>`<option value="${esc(m.model)}" ${selected===m.model?'selected':''}>${esc(m.label)}${m.is_default?' · по умолчанию':''}</option>`).join('');
}

const effortLabels={none:'без рассуждения',minimal:'минимальный',low:'низкий',medium:'средний',high:'высокий',xhigh:'очень высокий',max:'максимальный',ultra:'ультра'};
export function preferredReasoningEffort(models,selectedModel,selectedEffort=''){
 const model=(models||[]).find(m=>selectedModel?m.model===selectedModel:m.is_default);
 const supported=[...new Set((model?.efforts||[]).filter(e=>Object.hasOwn(effortLabels,e)))];
 if(supported.includes(selectedEffort))return selectedEffort;
 if(model?.model==='gpt-6-sol'&&supported.includes('xhigh'))return 'xhigh';
 if(selectedEffort==='auto'&&supported.includes('high'))return 'high';
 if(supported.includes(model?.default_effort))return model.default_effort;
 return supported[0]||'';
}
export function reasoningOptions(models,selectedModel,selectedEffort=''){
 const model=(models||[]).find(m=>selectedModel?m.model===selectedModel:m.is_default);
 const supported=[...new Set((model?.efforts||[]).filter(e=>Object.hasOwn(effortLabels,e)))];
 const selected=preferredReasoningEffort(models,selectedModel,selectedEffort);
 return supported.length?supported.map(e=>`<option value="${e}" ${selected===e?'selected':''}>Уровень · ${effortLabels[e]}</option>`).join(''):'<option value="" disabled selected>Уровни недоступны</option>';
}
