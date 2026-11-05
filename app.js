'use strict';
window.APP={
  id:'lq',name:'LineageIQ',
  logo:'<span class="mk"><svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="5" cy="6" r="2.5"/><circle cx="5" cy="18" r="2.5"/><circle cx="19" cy="12" r="2.5"/><path d="M7.3 7.3l9 3.5M7.3 16.7l9-3.5"/></svg></span>',
  nav:[['index.html','home','Home'],['assets.html','grid','Assets'],['incidents.html','alert','Incidents'],['impact.html','impact','Impact'],['settings.html','settings','Settings']],
  accents:[['#5b8cff','#7c5cff','#35d6e8'],['#35d6e8','#5b8cff','#7c5cff'],['#7c5cff','#5b8cff','#35d6e8'],['#4fe0a0','#5b8cff','#35d6e8']],
  cta:['assets.html','grid','Explore assets'],
  icons:{
    grid:'M4 4h7v7H4zM13 4h7v7h-7zM4 13h7v7H4zM13 13h7v7h-7z',
    alert:'M12 3l9 16H3zM12 10v4M12 17v.5',
    impact:'M12 12m-3 0a3 3 0 106 0 3 3 0 10-6 0M12 2v4M12 18v4M2 12h4M18 12h4M5 5l2.5 2.5M16.5 16.5L19 19M19 5l-2.5 2.5M7.5 16.5L5 19',
    table:'M3 5h18v14H3zM3 10h18M9 5v14',
    chart:'M3 12h4l2-7 4 14 2-7h6'
  },
  ensureSeed(){}
};
/* ---------------- data warehouse DAG ---------------- */
// layers: raw(0) -> staging(1) -> mart(2) -> bi(3)
const ASSETS=[
 // raw
 {id:'ext_stripe',name:'ext_stripe_charges',layer:'raw',type:'ingested table',up:[],owner:'data-eng',rows:4.2e6,sla:6,fresh:2.1,consumers:0,cols:[['charge_id','string',[['unique',1],['not_null',1]]],['amount','number',[['not_null',1]]],['customer_id','string',[['not_null',1]]],['created','timestamp',[['not_null',1]]]]},
 {id:'ext_app_events',name:'ext_app_events',layer:'raw',type:'event stream',up:[],owner:'data-eng',rows:88e6,sla:1,fresh:0.4,consumers:0,cols:[['event_id','string',[['unique',1]]],['user_id','string',[['not_null',1]]],['event_type','string',[['accepted_values',1]]],['ts','timestamp',[['not_null',1]]]]},
 {id:'ext_crm',name:'ext_salesforce_accounts',layer:'raw',type:'ingested table',up:[],owner:'rev-ops',rows:52e3,sla:24,fresh:31,consumers:0,cols:[['account_id','string',[['unique',1]]],['name','string',[['not_null',1]]],['tier','string',[['accepted_values',0]]],['region','string',[['not_null',1]]]]},
 {id:'ext_subs',name:'ext_subscriptions',layer:'raw',type:'ingested table',up:[],owner:'data-eng',rows:61e3,sla:6,fresh:3.0,consumers:0,cols:[['sub_id','string',[['unique',1]]],['account_id','string',[['not_null',1]]],['plan','string',[['accepted_values',1]]],['mrr','number',[['not_null',1]]]]},
 // staging
 {id:'stg_payments',name:'stg_payments',layer:'staging',type:'model',up:['ext_stripe'],owner:'analytics-eng',rows:4.2e6,sla:6,fresh:2.3,consumers:0,cols:[['payment_id','string',[['unique',1]]],['amount_usd','number',[['not_null',1]]],['customer_id','string',[['not_null',1]]],['paid_at','timestamp',[['not_null',1]]]]},
 {id:'stg_events',name:'stg_events',layer:'staging',type:'model',up:['ext_app_events'],owner:'analytics-eng',rows:88e6,sla:1,fresh:0.6,consumers:0,cols:[['event_id','string',[['unique',1]]],['user_id','string',[['not_null',1]]],['event_type','string',[['accepted_values',1]]],['event_ts','timestamp',[['not_null',1]]]]},
 {id:'stg_accounts',name:'stg_accounts',layer:'staging',type:'model',up:['ext_crm','ext_subs'],owner:'analytics-eng',rows:52e3,sla:24,fresh:31,consumers:0,cols:[['account_id','string',[['unique',1]]],['name','string',[['not_null',1]]],['tier','string',[['accepted_values',0]]],['mrr','number',[['not_null',1]]],['region','string',[['not_null',1]]]]},
 // mart
 {id:'fct_revenue',name:'fct_revenue',layer:'mart',type:'model',up:['stg_payments','stg_accounts'],owner:'analytics-eng',rows:1.1e6,sla:6,fresh:2.5,consumers:0,cols:[['date','date',[['not_null',1]]],['account_id','string',[['not_null',1]]],['revenue_usd','number',[['not_null',1]]],['region','string',[['not_null',1]]]]},
 {id:'dim_customer',name:'dim_customer',layer:'mart',type:'model',up:['stg_accounts','stg_events'],owner:'analytics-eng',rows:52e3,sla:24,fresh:31,consumers:0,cols:[['customer_id','string',[['unique',1]]],['tier','string',[['accepted_values',0]]],['signup_date','date',[['not_null',1]]],['last_seen','timestamp',[['not_null',1]]]]},
 {id:'fct_engagement',name:'fct_engagement',layer:'mart',type:'model',up:['stg_events'],owner:'product-analytics',rows:12e6,sla:1,fresh:0.7,consumers:0,cols:[['user_id','string',[['not_null',1]]],['dau_flag','boolean',[['not_null',1]]],['sessions','number',[['not_null',1]]],['date','date',[['not_null',1]]]]},
 {id:'fct_churn',name:'fct_churn_features',layer:'mart',type:'model',up:['fct_revenue','dim_customer','fct_engagement'],owner:'ml-platform',rows:52e3,sla:24,fresh:31,consumers:0,cols:[['customer_id','string',[['unique',1]]],['mrr','number',[['not_null',1]]],['engagement_30d','number',[['not_null',1]]],['churned','boolean',[['not_null',1]]]]},
 // bi
 {id:'bi_exec',name:'Executive Revenue Dashboard',layer:'bi',type:'dashboard',up:['fct_revenue'],owner:'finance',rows:0,sla:6,fresh:2.5,consumers:140,cols:[]},
 {id:'bi_growth',name:'Growth & Engagement Dashboard',layer:'bi',type:'dashboard',up:['fct_engagement','dim_customer'],owner:'growth',rows:0,sla:1,fresh:0.8,consumers:85,cols:[]},
 {id:'bi_churn',name:'Churn Risk Model',layer:'bi',type:'ml model',up:['fct_churn'],owner:'ml-platform',rows:0,sla:24,fresh:31,consumers:12,cols:[]},
 {id:'bi_board',name:'Board KPI Report',layer:'bi',type:'report',up:['fct_revenue','dim_customer'],owner:'finance',rows:0,sla:24,fresh:4,consumers:30,cols:[]}
];
const LAYERS=['raw','staging','mart','bi'];
function asset(id){return ASSETS.find(a=>a.id===id);}
function downstream(id){const a=asset(id);const out=new Set();const q=[id];while(q.length){const cur=q.shift();ASSETS.forEach(x=>{if(x.up.includes(cur)&&!out.has(x.id)){out.add(x.id);q.push(x.id);}});}return [...out];}
function upstream(id){const out=new Set();const q=[id];while(q.length){const cur=q.shift();(asset(cur).up||[]).forEach(u=>{if(!out.has(u)){out.add(u);q.push(u);}});}return [...out];}
function health(a){ // breached SLA or failed test => bad/warn
  const late=a.fresh>a.sla;const failedTests=(a.cols||[]).reduce((s,c)=>s+c[2].filter(t=>!t[1]).length,0);
  if(failedTests>0)return 'bad';if(late&&a.fresh>a.sla*1.3)return 'bad';if(late)return 'warn';
  // inherit worst upstream
  return 'ok';
}
function healthDeep(id){let h=health(asset(id));upstream(id).forEach(u=>{const uh=health(asset(u));if(uh==='bad')h='bad';else if(uh==='warn'&&h==='ok')h='warn';});return h;}
/* ---------------- incidents (derived + propagated) ---------------- */
function buildIncidents(){const out=[];
  ASSETS.forEach(a=>{
    const failed=(a.cols||[]).filter(c=>c[2].some(t=>!t[1]));
    failed.forEach(c=>{const t=c[2].find(x=>!x[1]);out.push({type:'failed test',sev:'bad',asset:a.id,title:a.name+' — '+t[0]+' test failing on '+c[0],detail:'dbt test '+t[0]+'('+c[0]+') returned failing rows',ago:'22m ago',blast:downstream(a.id).length});});
    if(a.fresh>a.sla){const sev=a.fresh>a.sla*1.3?'bad':'warn';out.push({type:'freshness',sev,asset:a.id,title:a.name+' is stale',detail:'last updated '+a.fresh.toFixed(1)+'h ago · SLA '+a.sla+'h',ago:'1h ago',blast:downstream(a.id).length});}
  });
  // a couple of synthetic volume/schema anomalies
  out.push({type:'volume anomaly',sev:'warn',asset:'stg_events',title:'stg_events row count dropped 38%',detail:'54.6M rows vs 88M 7-day avg — possible upstream gap',ago:'3h ago',blast:downstream('stg_events').length});
  out.push({type:'schema change',sev:'warn',asset:'ext_stripe',title:'ext_stripe_charges added column `fee_amount`',detail:'new column detected — downstream models may ignore it',ago:'6h ago',blast:downstream('ext_stripe').length});
  const order={bad:0,warn:1,ok:2};return out.sort((a,b)=>order[a.sev]-order[b.sev]||b.blast-a.blast);
}
/* ---------------- lineage graph renderer ---------------- */
function renderLineage(container,svgEl,focusId){
  const ups=upstream(focusId),downs=downstream(focusId);
  const nodes=[focusId,...ups,...downs];const set=new Set(nodes);
  // assign column by layer index relative; use distance from focus
  function dist(id){if(id===focusId)return 0;if(ups.includes(id))return -levelUp(id);return levelDown(id);}
  function levelUp(id){let d=0,cur=id;const seen=new Set();while(true){const a=asset(cur);const parentInSet=(a.up||[]).find(u=>u===focusId||ups.includes(u));if(!a.up.length)return d+1;d++;if(d>6)return d;cur=a.up[0];if(cur===focusId)return d;}}
  function levelDown(id){ // BFS depth from focus
    let depth=1;let frontier=ASSETS.filter(x=>x.up.includes(focusId)).map(x=>x.id);const seen=new Set(frontier);
    while(frontier.length){if(frontier.includes(id))return depth;const next=[];frontier.forEach(f=>ASSETS.forEach(x=>{if(x.up.includes(f)&&!seen.has(x.id)){seen.add(x.id);next.push(x.id);}}));frontier=next;depth++;if(depth>8)break;}return depth;
  }
  // columns by distance
  const colMap={};nodes.forEach(id=>{const d=dist(id);(colMap[d]=colMap[d]||[]).push(id);});
  const cols=Object.keys(colMap).map(Number).sort((a,b)=>a-b);
  const W=Math.max(700,cols.length*180),H=360;container.style.minWidth=W+'px';container.style.height=H+'px';
  const pos={};cols.forEach((c,ci)=>{const ids=colMap[c];const x=cols.length===1?W/2:(ci/(cols.length-1))*(W-160)+80;
    ids.forEach((id,ri)=>{const y=(ids.length===1?0.5:ri/(ids.length-1))*(H-100)+50;pos[id]={x,y:ids.length===1?H/2:y};});});
  // nodes html
  container.querySelectorAll('.lnode').forEach(n=>n.remove());
  nodes.forEach(id=>{const a=asset(id);const h=healthDeep(id);const el=document.createElement('a');el.href='asset.html?a='+id;el.className='lnode '+h+(id===focusId?' focus':'');
    el.style.left=pos[id].x+'px';el.style.top=pos[id].y+'px';
    el.innerHTML='<div class="ln"><span class="hdot '+h+'" style="width:7px;height:7px"></span>'+esc(a.name.length>20?a.name.slice(0,19)+'…':a.name)+'</div><div class="lp">'+a.layer+' · '+a.type+'</div>';
    container.appendChild(el);});
  // edges
  let paths='';nodes.forEach(id=>{(asset(id).up||[]).forEach(u=>{if(set.has(u)){const a=pos[u],b=pos[id];if(!a||!b)return;const mx=(a.x+b.x)/2;
    const hot=(id===focusId||u===focusId);paths+='<path class="'+(hot?'hot':'')+'" d="M'+a.x+' '+a.y+' C'+mx+' '+a.y+' '+mx+' '+b.y+' '+b.x+' '+b.y+'"/>';}});});
  svgEl.innerHTML=paths;svgEl.setAttribute('viewBox','0 0 '+W+' '+H);svgEl.style.width=W+'px';svgEl.style.height=H+'px';
}
/* ---------------- custom 3D hero: pipeline flow ---------------- */
function mountPipelineHero(id){if(typeof THREE==='undefined')return;if(matchMedia('(prefers-reduced-motion: reduce)').matches)return;
  const cv=document.getElementById(id);if(!cv)return;const r=new THREE.WebGLRenderer({canvas:cv,antialias:true,alpha:true});
  const b=cv.getBoundingClientRect();let w=b.width||480,h=b.height||480;r.setPixelRatio(Math.min(2,devicePixelRatio));r.setSize(w,h,false);
  const sc=new THREE.Scene();const cam=new THREE.PerspectiveCamera(52,w/h,.1,100);cam.position.set(0,0,6.2);
  const cs=getComputedStyle(document.documentElement);const C=v=>new THREE.Color(cs.getPropertyValue(v).trim()||'#5b8cff');
  const acc=C('--acc'),acc2=C('--acc2'),amb=C('--amb');
  const grp=new THREE.Group();grp.rotation.x=0.15;sc.add(grp);
  const layers=[2,3,4,3];const xs=[-3.2,-1.1,1.1,3.2];const nodePos=[];const layerArr=[];
  layers.forEach((count,li)=>{const arr=[];for(let i=0;i<count;i++){const y=(count===1?0:(i/(count-1)-0.5))*3.2;const v=new THREE.Vector3(xs[li],y,(Math.random()-0.5)*0.4);v.li=li;nodePos.push(v);arr.push(v);}layerArr.push(arr);});
  const parr=[],pcol=[];nodePos.forEach(v=>{parr.push(v.x,v.y,v.z);const c=[amb,acc,acc2,acc2][v.li];pcol.push(c.r,c.g,c.b);});
  const pg=new THREE.BufferGeometry();pg.setAttribute('position',new THREE.Float32BufferAttribute(parr,3));pg.setAttribute('color',new THREE.Float32BufferAttribute(pcol,3));
  grp.add(new THREE.Points(pg,new THREE.PointsMaterial({size:.22,vertexColors:true,transparent:true,opacity:1,blending:THREE.AdditiveBlending,depthWrite:false})));
  const edges=[];for(let li=0;li<layerArr.length-1;li++)layerArr[li].forEach(a=>layerArr[li+1].forEach(b2=>{if(Math.random()<0.7)edges.push([a,b2]);}));
  const larr=[];edges.forEach(([a,b2])=>larr.push(a.x,a.y,a.z,b2.x,b2.y,b2.z));
  const lg=new THREE.BufferGeometry();lg.setAttribute('position',new THREE.Float32BufferAttribute(larr,3));
  grp.add(new THREE.LineSegments(lg,new THREE.LineBasicMaterial({color:acc,transparent:true,opacity:.22,blending:THREE.AdditiveBlending})));
  // flowing particles along edges (left->right data flow)
  const PP=edges.length*2;const flowArr=new Float32Array(PP*3);const flowGeo=new THREE.BufferGeometry();flowGeo.setAttribute('position',new THREE.Float32BufferAttribute(flowArr,3));
  const flow=new THREE.Points(flowGeo,new THREE.PointsMaterial({color:amb,size:.12,transparent:true,opacity:.95,blending:THREE.AdditiveBlending,depthWrite:false}));grp.add(flow);
  let mX=0,mY=0;cv.addEventListener('pointermove',e=>{const r2=cv.getBoundingClientRect();mX=(e.clientX-r2.left)/r2.width-.5;mY=(e.clientY-r2.top)/r2.height-.5;});
  const t0=performance.now();
  (function loop(now){const t=(now-t0)/1000;
    for(let i=0;i<edges.length;i++){const[a,b2]=edges[i];for(let k=0;k<2;k++){const f=((t*0.35+i*0.13+k*0.5)%1);const idx=(i*2+k)*3;flowArr[idx]=a.x+(b2.x-a.x)*f;flowArr[idx+1]=a.y+(b2.y-a.y)*f;flowArr[idx+2]=a.z+(b2.z-a.z)*f;}}
    flowGeo.attributes.position.needsUpdate=true;
    grp.rotation.y=Math.sin(t*0.2)*0.25+mX*0.4;grp.rotation.x=0.15+mY*0.2;
    r.render(sc,cam);requestAnimationFrame(loop);})(t0);
  addEventListener('resize',()=>{const b2=cv.getBoundingClientRect();w=b2.width;h=b2.height;cam.aspect=w/h;cam.updateProjectionMatrix();r.setSize(w,h,false);});
}
/* ---------------- helpers ---------------- */
function statRow(c){return c.map(([l,n])=>'<div class="bstat"><div class="n">'+n+'</div><div class="l">'+l+'</div></div>').join('');}
function marq(t){const s='<span>'+t.map(x=>'<b>'+x+'</b>').join(' · ')+' · </span>';return s+s;}
function featGrid(items){return items.map(([h,ic,t,d],i)=>'<a class="fcell" href="'+h+'"><div class="fi">'+icon(ic,22)+'</div><div class="fx">0'+(i+1)+'</div><h3>'+t+'</h3><p>'+d+'</p><div class="go">Open '+icon('chevron',13)+'</div></a>').join('');}
function fmtRows(n){return n>=1e6?(n/1e6).toFixed(1)+'M':n>=1e3?(n/1e3).toFixed(0)+'k':n;}
const FEATURES="data lineage graph · 15 assets across 4 layers (raw→staging→mart→BI) · column-level schema · dbt-style tests (not_null, unique, accepted_values) · test pass/fail · freshness & SLA tracking · staleness detection · health propagation downstream · interactive lineage DAG · upstream & downstream traversal · asset catalog · search & layer filter · owner & row counts · incident feed · freshness / failed-test / volume / schema-change incidents · severity & blast radius · impact analyzer · pick-an-asset blast radius · BFS downstream set · affected dashboards & consumers · estimated users hit · grouped-by-layer impact · freshness sparkline · 3D pipeline-flow hero · flowing data particles · cursor-reactive hero · ambient motion · scroll reveals · custom cursor · light & dark · 4 accents · keyboard theme toggle · JSON export · fully offline · responsive · award-grade UI";
/* ---------------- PAGES ---------------- */
window.PAGES={
 index(){
   const incs=buildIncidents();
   $('meta').innerHTML=statRow([['assets',ASSETS.length],['layers',4],['<span style="color:var(--rose)">incidents</span>',incs.filter(i=>i.sev==='bad').length],['consumers',ASSETS.reduce((s,a)=>s+a.consumers,0)]]);
   $('marq').innerHTML=marq(['data lineage','impact analysis','freshness SLAs','failing tests','blast radius','column-level','trusted data']);
   $('fgrid').innerHTML=featGrid([
     ['assets.html','grid','Asset catalog','Every table, model and dashboard — searchable, with health and owners.'],
     ['assets.html','table','Lineage graph','Walk upstream and downstream through an interactive DAG.'],
     ['incidents.html','alert','Incidents','Freshness breaches, failing tests and anomalies, ranked by blast radius.'],
     ['impact.html','impact','Impact analysis','Pick any asset and see exactly what breaks downstream.'],
     ['assets.html','chart','Freshness & tests','SLA tracking and dbt-style data tests on every model.'],
     ['settings.html','settings','Settings','Theme, accents and a private JSON export of the graph.']
   ]);
   $('featlist').textContent='50+ features — '+FEATURES+'.';
   mountPipelineHero('hero3d');
 },
 assets(){
   let q='',lf='all';
   function draw(){let list=ASSETS.filter(a=>(lf==='all'||a.layer===lf)&&(a.name.toLowerCase().includes(q)||a.owner.includes(q)));
     $('alist').innerHTML=list.map(a=>{const h=healthDeep(a.id);const dn=downstream(a.id).length;
       return '<a class="arow" href="asset.html?a='+a.id+'"><span class="hdot '+h+'"></span><div><div class="an">'+esc(a.name)+'</div><div class="ap">'+a.type+' · '+esc(a.owner)+'</div></div><span class="lyr '+a.layer+'">'+a.layer+'</span><div class="hidec"><div class="ap">freshness</div><div style="font-family:var(--mono);font-size:12px;color:'+(a.fresh>a.sla?'var(--rose)':'var(--ink2)')+'">'+a.fresh.toFixed(1)+'h / '+a.sla+'h SLA</div></div><div class="num">'+(a.rows?fmtRows(a.rows)+' rows':a.consumers+' users')+'<div class="ap">'+dn+' downstream</div></div></a>';}).join('');
   }
   $('q').oninput=()=>{q=$('q').value.toLowerCase().trim();draw();};
   $('lseg').querySelectorAll('button').forEach(b=>b.onclick=()=>{lf=b.dataset.l;$('lseg').querySelectorAll('button').forEach(x=>x.classList.toggle('on',x===b));draw();});
   const bad=ASSETS.filter(a=>healthDeep(a.id)==='bad').length;
   $('summary').innerHTML=statRow([['healthy',ASSETS.filter(a=>healthDeep(a.id)==='ok').length],['<span style="color:var(--gold)">degraded</span>',ASSETS.filter(a=>healthDeep(a.id)==='warn').length],['<span style="color:var(--rose)">unhealthy</span>',bad],['total rows',fmtRows(ASSETS.reduce((s,a)=>s+a.rows,0))]]);
   draw();
 },
 asset(){
   const id=new URLSearchParams(location.search).get('a')||ASSETS[0].id;const a=asset(id);const h=healthDeep(id);
   $('aname').textContent=a.name;$('ameta').textContent=a.type+' · '+a.layer+' layer · owner '+a.owner;
   $('atag').innerHTML='<span class="htag '+h+'">'+(h==='ok'?'healthy':h==='warn'?'degraded':'unhealthy')+'</span>';
   const dn=downstream(id),upn=upstream(id);
   $('astats').innerHTML=statRow([[a.rows?'rows':'consumers',a.rows?fmtRows(a.rows):a.consumers],['freshness',a.fresh.toFixed(1)+'h'],['SLA',a.sla+'h'],['downstream',dn.length]]);
   renderLineage($('lincanvas'),$('linsvg'),id);
   // columns + tests
   if(a.cols.length){$('colsCard').style.display='';$('cols').innerHTML=a.cols.map(c=>'<div class="col-row"><span class="cn">'+esc(c[0])+'</span><span class="ct">'+c[1]+'</span>'+c[2].map(t=>'<span class="testpill '+(t[1]?'ok':'bad')+'">'+(t[1]?icon('check',10):icon('close',10))+t[0]+'</span>').join(' ')+'</div>').join('');}
   else $('colsCard').style.display='none';
   // freshness sparkline (seeded)
   let seed=0;for(const ch of id)seed+=ch.charCodeAt(0);const rng=mulberry(seed);let fh='';for(let i=0;i<14;i++){const v=a.fresh*(0.5+rng()*1.1)*(i===13?1:1);const late=v>a.sla;fh+='<div class="fb'+(late?' late':'')+'" style="height:'+Math.min(100,v/(a.sla*2)*90)+'%" title="'+v.toFixed(1)+'h"></div>';}$('fresh').innerHTML=fh;
   // incidents for this asset
   const incs=buildIncidents().filter(i=>i.asset===id||dn.includes(i.asset)||upn.includes(i.asset));
   $('aincs').innerHTML=incs.length?incs.slice(0,5).map(i=>'<div class="inc '+i.sev+'"><span class="hdot '+i.sev+'" style="margin-top:5px"></span><div class="ib"><div class="it">'+esc(i.title)+'</div><div class="id2">'+esc(i.detail)+'</div></div><span class="ix">'+i.ago+'</span></div>').join(''):'<div class="empty">No incidents touching this asset.</div>';
   $('upcount').textContent=upn.length;$('downcount').textContent=dn.length;
 },
 incidents(){
   let sev='all';const all=buildIncidents();
   function draw(){const list=all.filter(i=>sev==='all'||i.sev===sev);
     $('ilist').innerHTML=list.map(i=>{const a=asset(i.asset);return '<a class="inc '+i.sev+'" href="asset.html?a='+i.asset+'"><span class="hdot '+i.sev+'" style="margin-top:5px"></span><div class="ib"><div class="it">'+esc(i.title)+'</div><div class="id2">'+esc(i.detail)+' · <span style="color:var(--acc)">'+i.blast+' downstream affected</span></div></div><div style="text-align:right"><span class="lyr" style="margin-bottom:4px;display:inline-block">'+i.type+'</span><div class="ix">'+i.ago+'</div></div></a>';}).join('');
   }
   $('sseg').querySelectorAll('button').forEach(b=>b.onclick=()=>{sev=b.dataset.s;$('sseg').querySelectorAll('button').forEach(x=>x.classList.toggle('on',x===b));draw();});
   $('icount').innerHTML=statRow([['<span style="color:var(--rose)">critical</span>',all.filter(i=>i.sev==='bad').length],['<span style="color:var(--gold)">warning</span>',all.filter(i=>i.sev==='warn').length],['total',all.length],['max blast',Math.max(...all.map(i=>i.blast))]]);
   draw();
 },
 impact(){
   let id=ASSETS.find(a=>a.layer==='raw').id;
   $('asel').innerHTML=ASSETS.map(a=>'<option value="'+a.id+'"'+(a.id===id?' selected':'')+'>'+esc(a.name)+' ('+a.layer+')</option>').join('');
   function draw(){id=$('asel').value;const a=asset(id);const dn=downstream(id);
     const dashes=dn.map(asset).filter(x=>x.layer==='bi');const marts=dn.map(asset).filter(x=>x.layer==='mart');
     const users=dashes.reduce((s,x)=>s+x.consumers,0);
     $('blast').innerHTML=[['<em>'+dn.length+'</em>','downstream assets'],['<em>'+dashes.length+'</em>','dashboards / models'],['<em>'+marts.length+'</em>','mart tables'],['<em>'+users+'</em>','users affected']].map(x=>'<div class="bcell"><div class="bn">'+x[0]+'</div><div class="bl">'+x[1]+'</div></div>').join('');
     $('impdesc').innerHTML='If <b>'+esc(a.name)+'</b> breaks or changes, everything below is at risk — traced through the lineage graph by breadth-first search.';
     const byLayer={};dn.forEach(d=>{const x=asset(d);(byLayer[x.layer]=byLayer[x.layer]||[]).push(x);});
     $('implist').innerHTML=LAYERS.filter(l=>byLayer[l]).map(l=>'<div style="margin-bottom:18px"><div class="eyebrow" style="margin-bottom:12px">'+l+' layer</div>'+byLayer[l].map(x=>'<a class="impitem" href="asset.html?a='+x.id+'"><span class="hdot '+healthDeep(x.id)+'"></span><div style="flex:1"><div class="an" style="font-family:var(--disp);font-weight:600">'+esc(x.name)+'</div><div class="ap" style="font-family:var(--mono);font-size:11px;color:var(--mut)">'+x.type+(x.consumers?' · '+x.consumers+' users':'')+'</div></div><span class="lyr '+x.layer+'">'+x.layer+'</span></a>').join('')+'</div>').join('')||'<div class="empty">This asset has no downstream dependencies — safe to change.</div>';
     $('impkv').innerHTML=[['Selected asset',a.name],['Layer',a.layer],['Direct children',ASSETS.filter(x=>x.up.includes(id)).length],['Total downstream',dn.length],['Dashboards hit',dashes.length],['Users affected',users]].map(r=>'<div class="kv"><span class="k">'+r[0]+'</span><span class="v">'+esc(String(r[1]))+'</span></div>').join('');
   }
   $('asel').onchange=draw;draw();
 },
 settings(){
   $('themeToggle').classList.toggle('on',theme()==='light');$('themeToggle').onclick=()=>{toggleTheme();$('themeToggle').classList.toggle('on',theme()==='light');};
   $('accents2').innerHTML=APP.accents.map((a,i)=>'<button class="ad'+(i===accentIdx()?' on':'')+'" data-i="'+i+'" data-press style="width:24px;height:24px;background:linear-gradient(135deg,'+a[0]+','+a[1]+')"></button>').join('');
   $('accents2').querySelectorAll('.ad').forEach(b=>b.onclick=()=>setAccent(+b.dataset.i));
   $('exp').onclick=()=>dl('lineageiq-graph.json','application/json',JSON.stringify(ASSETS.map(a=>({id:a.id,layer:a.layer,upstream:a.up,downstream:downstream(a.id),health:healthDeep(a.id)})),null,2));
   $('featlist').textContent=FEATURES+'.';
 }
};
function mulberry(a){return function(){a|=0;a=a+0x6D2B79F5|0;let t=Math.imul(a^a>>>15,1|a);t=t+Math.imul(t^t>>>7,61|t)^t;return((t^t>>>14)>>>0)/4294967296;};}
