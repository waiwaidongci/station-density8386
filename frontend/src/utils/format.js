export const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export const fmt=value=>typeof value==='number'?(Number.isInteger(value)?String(value):value.toLocaleString('zh-CN',{maximumFractionDigits:4})):esc(value);
export const formObject=form=>Object.fromEntries(new FormData(form));

export const localTime=value=>new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false}).format(new Date(value));
export const fullTime=value=>new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}).format(new Date(value));
// 已等待时长：入队到当前（未完成）或到开始/核对完成
export function waitSeconds(run,nowTs=Date.now()){
  if(!run.enqueued_at)return 0;
  const end=run.verified_at||(run.state==='running'?run.started_at:null)||null;
  const endTs=end?Date.parse(end):nowTs;
  return Math.max(0,Math.round((endTs-Date.parse(run.enqueued_at))/1000));
}
export function durationLabel(sec){
  if(sec<=0)return '0 秒';
  const m=Math.floor(sec/60),s=sec%60;
  return m?`${m} 分 ${s} 秒`:`${s} 秒`;
}
