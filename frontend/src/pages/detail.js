import {table} from '../components/common/table.js';
import {badge} from '../components/common/status.js';
import {esc} from '../utils/html.js';
import {download} from '../api/client.js';
import {message} from '../hooks/action.js';

function verdict(r){
 if(r.error)return `<section class="verdict error"><div><p>恢复被拒绝</p><h2>输入不合法</h2></div><p>${esc(r.error)}</p></section>`;
 const title=r.invariant_passed?(r.serializable?'全部提交可串行化':'冲突环已舍弃，无冲突提交保住'):'恢复失败';
 return `<section class="verdict"><div><p>恢复账本核对</p><h2>${esc(title)}</h2></div><p>检查点LSN ${r.checkpoint_lsn??'—'} · 崩溃点 ${r.crash_after??'—'}<br>重做 ${r.redo_count??0} 次 / 撤销 ${r.undo_count??0} 次<br>幸存提交 ${esc((r.survivors||[]).join(', ')||'无')} · 被舍弃 ${esc((r.sacrificed||[]).join(', ')||'无')}<br>未提交事务 ${esc((r.losers||[]).join(', ')||'无')}</p></section>`;
}
function checkPanel(r){
 const c=r.check;if(!c)return '';
 const rows=[];
 if(c.expected_order){
  rows.push(['录入串行顺序',esc(c.expected_order.join(' → ')),esc((r.serial_order||[]).join(' → ')),c.order_match?'<span class="ok">一致</span>':'<span class="bad">不一致</span>']);
 }
 if(c.expected_balances){
  const detail=Object.entries(c.expected_balances).map(([k,v])=>`${esc(k)}: 录入 ${esc(v)} / 实际 ${esc(r.recovered?.[k])}`).join('；');
  rows.push(['录入恢复余额',detail,'完整覆盖 '+(c.balances_complete?'是':'否'),(c.balances_match&&c.balances_complete)?'<span class="ok">核对通过</span>':'<span class="bad">核对不符</span>']);
 }
 if(!rows.length)return '<h2>值班核对</h2><p class="notice">本次未录入顺序或余额，仅展示引擎结论。可在恢复队列重新发起并录入核对值。</p>';
 return `<h2>值班录入核对</h2>${table(['核对项','录入值','引擎结果','结论'],rows)}`;
}
function cyclePanel(r){
 if(!r.conflict_cycles||!r.conflict_cycles.length)return `<h2>串行化分析</h2><p class="notice ok">全部已提交事务可排在同一先后顺序：<b>${esc((r.serial_order||[]).join(' → ')||'无提交')}</b>，串行重放已重现全部区间读命中点（验证${r.replay_verified?'通过':'失败'}）。</p>`;
 return `<h2>串行化分析 · 存在冲突环</h2><p class="notice error">以下环无法用任何单一先后顺序解释区间读命中，已按“最大保留、字典序确定”舍弃最少事务：</p>${table(['冲突环（事务）','被舍弃事务','幸存串行顺序','重放验证'],r.conflict_cycles.map(cyc=>[esc(cyc.join(' → ')+' → '+cyc[0]),esc((r.sacrificed||[]).join(', ')||'无'),esc((r.serial_order||[]).join(' → ')),r.replay_verified?'<span class="ok">通过</span>':'<span class="bad">失败</span>']))}`;
}
function readsPanel(r){
 const obs=r.read_observations||[];
 if(!obs.length)return '';
 return `<h2>区间读观察（读己之所写，不见他人未提交值）</h2>${table(['LSN','事务','区间','命中点（键:值）'],obs.map(o=>[o.lsn,esc(o.tx),`${o.low} ~ ${o.high}`,esc(Object.entries(o.hits).map(([k,v])=>`${k}:${v}`).join(', ')||'空（无命中）')]))}`;
}
export function detailPage(job){if(!job)return '<section class="empty"><h2>还没有恢复记录</h2><p>先选择故障场景并提交恢复任务，再逐条查看日志重做与撤销。</p><a href="#runs">进入恢复队列 →</a></section>';const r=job.result;return `<div class="section-title"><h2>${esc(job.label)} <small>#${job.id}</small></h2>${badge(job.status)}</div>${r?`${verdict(r)}${r.error?'':checkPanel(r)+cyclePanel(r)+readsPanel(r)+table(['账本键','检查点值','重做后','恢复后','独立幸存结果'],Object.keys(r.recovered||{}).map(k=>[esc(k),r.durable_checkpoint[k],r.after_redo[k],r.recovered[k],r.oracle[k]]))+'<h2>日志恢复轨迹</h2>'+table(['LSN','事务','阶段','键','before → after'],(r.trace||[]).map(t=>[t.lsn,esc(t.tx),esc(t.phase),esc(t.key),t.phase==='undo_loser'||t.phase==='undo_sacrificed'||t.phase==='redo_abort_undo'?`${t.after} → ${t.before}`:`${t.before} → ${t.after}`]))+'<p class="checksum">输入摘要 '+esc(r.input_sha256||'无')+'<br>结果摘要 '+esc(r.result_sha256||'无')+'</p>'}`:'<p class="notice">后台恢复计算中，请刷新状态。</p>'}<div class="toolbar"><button id="export">导出日志与恢复证据</button><button class="secondary" id="refresh">刷新状态</button></div><details><summary>查看冻结输入（含录入的顺序与余额）</summary><pre>${esc(JSON.stringify(job.snapshot,null,2))}</pre></details>`;}
export function bindDetail(job,render){document.querySelector('#refresh')?.addEventListener('click',render);document.querySelector('#export')?.addEventListener('click',()=>download(job.id).catch(e=>message(e.message,true)));}
