import {api} from './client.js';
export const sourcesApi={list:()=>api('/sources'),create:data=>api('/sources',{method:'POST',body:JSON.stringify(data)}),update:(id,data)=>api('/sources/'+id,{method:'PUT',body:JSON.stringify(data)}),records:id=>api('/sources/'+id+'/records'),add:(id,records)=>api('/sources/'+id+'/records',{method:'POST',body:JSON.stringify({records})})};
// 同一请求重试不新增研究：每次表单提交生成一个幂等键，网络重试/重复点击复用同一键
const newKey=()=>(crypto.randomUUID?crypto.randomUUID():'run-'+Date.now()+'-'+Math.random().toString(16).slice(2));
export const runsApi={list:()=>api('/runs'),get:id=>api('/runs/'+id),create:(data,idempotencyKey)=>api('/runs',{method:'POST',body:JSON.stringify(data),headers:{'Idempotency-Key':idempotencyKey||newKey()}})};
export const notesApi={list:()=>api('/notes'),create:data=>api('/notes',{method:'POST',body:JSON.stringify(data)}),approve:id=>api('/notes/'+id+'/approve',{method:'POST'})};
