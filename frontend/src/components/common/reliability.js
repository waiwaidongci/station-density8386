import {esc} from '../../utils/format.js';
import {waitSeconds,durationLabel,fullTime} from '../../utils/format.js';
// 任务可靠性面板：等待时长、执行/接管次数、核对结论与最终状态，供复访人员判断结果能否采用
export function reliabilityPanel(run){
  const waited=waitSeconds(run);
  const takeover=run.takeovers||0;
  const verified=!!run.verified_at;
  const rows=[
    ['当前状态',run.state==='completed'?'已完成并开放采用':run.state==='failed'?'计算失败':run.state==='running'?'正在计算（含崩溃接管续算）':'队列等待中'],
    ['队列等待',durationLabel(waited)+(run.enqueued_at?'（自 '+fullTime(run.enqueued_at)+'）':'')],
    ['执行次数',String(run.attempt_id||0)+' 次'+(takeover?` · 其中超时接管 ${takeover} 次`:' · 无接管')],
  ];
  if(run.started_at)rows.push(['最近开始',fullTime(run.started_at)]);
  if(run.heartbeat_at && run.state==='running')rows.push(['最近心跳',fullTime(run.heartbeat_at)]);
  if(verified){
    rows.push(['结果核对','结果与完整导出 CSV 双向核对一致 ✓']);
    rows.push(['核对时间',fullTime(run.verified_at)]);
    rows.push(['结果校验',(run.result_checksum||'').slice(0,16)+'…']);
  }else if(run.state==='failed'){
    rows.push(['结果核对','未通过核对，结果不可采用']);
  }else{
    rows.push(['结果核对','待计算完成后核对，未核对前不可采用']);
  }
  return '<div class="reliability"><p class="eyebrow">RELIABILITY / 执行可靠性</p><dl>'+
    rows.map(([k,v])=>'<div><dt>'+esc(k)+'</dt><dd>'+esc(v)+'</dd></div>').join('')+
    (takeover?'<p class="fine warn">检测到进程崩溃或超时，系统已沿用冻结输入自动接管重算，最终结果仅来自一次有效执行。</p>':'')+
    '</dl></div>';
}
