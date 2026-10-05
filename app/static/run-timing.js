export function duration(seconds){
 const minutes=Math.max(1,Math.ceil(seconds/60));
 return minutes<60?`${minutes} мин`:`${Math.floor(minutes/60)} ч${minutes%60?' '+minutes%60+' мин':''}`;
}
export function timingText(job,delta=0){
 const t=job.timing;if(!t)return '';
 const elapsed=t.elapsed_seconds+(t.active?Math.max(0,delta):0);
 const spent=elapsed<60?'меньше минуты':duration(elapsed);
 if(!t.active)return `Длительность: ${spent}`;
 let eta='Рассчитываем оставшееся время…';
 if(t.delayed)eta='Дольше ожидаемого · уточняем прогноз';
 else if(t.remaining_high!==null){
  const low=Math.max(0,t.remaining_low-delta),high=Math.max(0,t.remaining_high-delta);
  if(high<=0)eta='Уточняем прогноз…';
  else if(high<60)eta='Осталось примерно меньше минуты';
  else if(duration(low)===duration(high))eta=`Осталось примерно ${duration(high)}`;
  else eta=`Осталось примерно ${duration(low)} – ${duration(high)}`;
 }
 const label={collecting:'Чатов прочитано',extracting:'Блоков обработано',merging:'Блоков объединено',sending:'Шагов отправки выполнено'}[t.phase];
 return `${eta} · Прошло ${spent}${label&&t.total?' · '+label+': '+t.done+'/'+t.total:''}${t.active_units>1?' · В работе: '+t.active_units:''}`;
}
