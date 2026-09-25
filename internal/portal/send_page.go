package portal

const sendPageHTML = `<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Resumexfer</title><style>
body{font-family:system-ui,sans-serif;max-width:760px;margin:32px auto;padding:0 18px;color:#1f2328}
a{color:#0969da}li{margin:10px 0}.box{border:1px solid #d0d7de;border-radius:12px;padding:18px}
.muted{color:#656d76}button,a.button{display:inline-block;padding:10px 16px;border-radius:8px;background:#0969da;color:white;text-decoration:none;border:0;font-size:16px}
button:disabled{opacity:.6}progress{width:100%;height:18px;margin-top:12px}
</style></head><body><h1>Resumexfer</h1><div class="box">
{{if .Managed}}{{if .Single}}<p>This interrupted USB file will restart over Wi-Fi in the browser.</p>{{else}}<p>This interrupted USB job has {{.PendingFiles}} remaining files. Android's zero-install browser cannot recreate the original MTP folder directly, so Resumexfer will download the remaining files as one ZIP with their folder paths preserved.</p>{{end}}{{else}}<p>Files shared from the nearby computer. No app is required.</p>{{end}}
{{if .Single}}
<p><strong id="fileName">{{(index .Files 0).Name}}</strong></p>
<p><button id="download" data-name="{{(index .Files 0).Name}}" data-size="{{(index .Files 0).Size}}">Download</button></p>
<progress id="progress" value="0" max="1"></progress>
<p id="status" class="muted">Ready to download.</p>
<a id="httpFallback" data-path="file/0" href="file/0" download hidden>Download file</a>
{{else}}
<p><a class="button" data-path="all.zip" href="all.zip">{{if .Managed}}Download remaining {{.PendingFiles}} files as ZIP{{else}}Download all as ZIP{{end}}</a></p>
<ul>{{range $i,$f:=.Files}}<li><a data-path="file/{{$i}}" href="file/{{$i}}">{{$f.ZipName}}</a> <span class="muted">({{$f.Size}} bytes)</span></li>{{end}}</ul>
{{end}}
<p><button id="pauseTransfer" type="button">Pause</button> <button id="cancelTransfer" type="button">Cancel</button></p>
<p id="route" class="muted">Using the current local-network path.</p></div>
<script>
const routeStatus=document.getElementById('route'),pauseControl=document.getElementById('pauseTransfer'),cancelControl=document.getElementById('cancelTransfer'),transferStatus=document.getElementById('status');
let routes=[new URL('./',window.location.href).href],preferred=routes[0],serverPaused=false,cancelled=false,activeFastCancel=null;
function mergeRoutes(values){for(const value of values||[]){try{const base=new URL(value,window.location.href).href;if(!routes.includes(base))routes.push(base)}catch(e){}}}
function retargetDownloads(base){for(const link of document.querySelectorAll('[data-path]')){if(link.tagName==='A')link.href=new URL(link.dataset.path,base).href}}
async function routeFetch(path,options={},timeoutMs=10000){const ordered=[preferred,...routes.filter(item=>item!==preferred)];let lastError=null;for(const base of ordered){const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),timeoutMs);try{const response=await fetch(new URL(path,base),{...options,signal:controller.signal,cache:'no-store'});clearTimeout(timer);preferred=base;return response}catch(e){clearTimeout(timer);lastError=e}}throw lastError||new Error('No Resumexfer route is reachable')}
async function refreshRoutes(){if(cancelled)return;const ordered=[preferred,...routes.filter(item=>item!==preferred)];let lastError=null;for(const base of ordered){const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),2500);try{const response=await fetch(new URL('api/status',base),{cache:'no-store',signal:controller.signal});clearTimeout(timer);if(!response.ok)throw new Error('HTTP '+response.status);const info=await response.json();preferred=base;mergeRoutes(info.urls);retargetDownloads(preferred);serverPaused=!!info.paused;pauseControl.textContent=serverPaused?'Resume':'Pause';routeStatus.textContent=serverPaused?'Paused':'Using '+new URL(preferred).host;lastError=null;break}catch(e){clearTimeout(timer);lastError=e}}if(lastError)routeStatus.textContent='Current path is unavailable. Waiting for Resumexfer…';setTimeout(refreshRoutes,1500)}
async function sessionControl(path){const response=await routeFetch(path,{method:'POST'},5000);const text=await response.text();if(!response.ok)throw new Error(text||('HTTP '+response.status));return text?JSON.parse(text):{}}
pauseControl.onclick=async()=>{if(cancelled)return;pauseControl.disabled=true;const next=!serverPaused;try{await sessionControl(next?'api/pause':'api/resume');serverPaused=next;pauseControl.textContent=serverPaused?'Resume':'Pause';routeStatus.textContent=serverPaused?'Paused':'Resuming transfer…'}catch(e){routeStatus.textContent='Could not change transfer state: '+e.message}finally{if(!cancelled)pauseControl.disabled=false}};
cancelControl.onclick=async()=>{if(cancelled)return;cancelled=true;pauseControl.disabled=true;cancelControl.disabled=true;if(activeFastCancel)activeFastCancel();try{await sessionControl('api/cancel')}catch(e){}routeStatus.textContent='Cancelled.';if(transferStatus)transferStatus.textContent='Cancelled.'};
retargetDownloads(preferred);refreshRoutes();

const downloadButton=document.getElementById('download');
if(downloadButton){
const status=document.getElementById('status'),progress=document.getElementById('progress'),fallback=document.getElementById('httpFallback');
const fileName=downloadButton.dataset.name,fileSize=Number(downloadButton.dataset.size||0);
const DB='resumexfer-fast-v1',PARTS='parts',META='meta',BLOCK=16*1024*1024,LARGE_DIRECT=1536*1024*1024,STALE=24*60*60*1000;
const transferKey=window.location.pathname.replace(/\/+$/,'')+'|0';

function reqPromise(req){return new Promise((resolve,reject)=>{req.onsuccess=()=>resolve(req.result);req.onerror=()=>reject(req.error||new Error('Browser storage failed'))})}
function txPromise(tx){return new Promise((resolve,reject)=>{tx.oncomplete=()=>resolve();tx.onerror=()=>reject(tx.error||new Error('Browser storage failed'));tx.onabort=()=>reject(tx.error||new Error('Browser storage aborted'))})}
function openDB(){return new Promise((resolve,reject)=>{const req=indexedDB.open(DB,1);req.onupgradeneeded=()=>{const db=req.result;if(!db.objectStoreNames.contains(PARTS)){const store=db.createObjectStore(PARTS,{keyPath:'key'});store.createIndex('transfer','transfer',{unique:false})}if(!db.objectStoreNames.contains(META))db.createObjectStore(META,{keyPath:'transfer'})};req.onsuccess=()=>resolve(req.result);req.onerror=()=>reject(req.error||new Error('Browser storage unavailable'))})}
async function deleteTransfer(db,key){const tx=db.transaction([PARTS,META],'readwrite'),parts=tx.objectStore(PARTS),index=parts.index('transfer');await new Promise((resolve,reject)=>{const req=index.openCursor(IDBKeyRange.only(key));req.onsuccess=()=>{const cursor=req.result;if(!cursor){resolve();return}cursor.delete();cursor.continue()};req.onerror=()=>reject(req.error)});tx.objectStore(META).delete(key);await txPromise(tx)}
async function cleanupStale(db){const tx=db.transaction(META,'readonly'),all=await reqPromise(tx.objectStore(META).getAll());await txPromise(tx);const cutoff=Date.now()-STALE;for(const item of all||[]){if((item.updated||0)<cutoff){try{await deleteTransfer(db,item.transfer)}catch(e){}}}}
async function readState(db){const tx=db.transaction(META,'readonly'),meta=await reqPromise(tx.objectStore(META).get(transferKey));await txPromise(tx);if(!meta)return {offset:0,nextIndex:0};if(meta.name!==fileName||meta.size!==fileSize||meta.offset<0||meta.offset>fileSize){await deleteTransfer(db,transferKey);return {offset:0,nextIndex:0}}return {offset:meta.offset||0,nextIndex:meta.nextIndex||0}}
async function storeBlock(db,index,start,end,blob){const tx=db.transaction([PARTS,META],'readwrite');tx.objectStore(PARTS).put({key:transferKey+':'+index,transfer:transferKey,index,start,end,blob});tx.objectStore(META).put({transfer:transferKey,name:fileName,size:fileSize,offset:end,nextIndex:index+1,updated:Date.now()});await txPromise(tx)}
async function loadParts(db){const tx=db.transaction(PARTS,'readonly'),rows=await reqPromise(tx.objectStore(PARTS).index('transfer').getAll(transferKey));await txPromise(tx);rows.sort((a,b)=>a.index-b.index);return rows.map(row=>row.blob)}
function mimeForName(name){const n=name.toLowerCase();if(n.endsWith('.mp4'))return 'video/mp4';if(n.endsWith('.mkv'))return 'video/x-matroska';if(n.endsWith('.webm'))return 'video/webm';if(n.endsWith('.mp3'))return 'audio/mpeg';if(n.endsWith('.pdf'))return 'application/pdf';if(n.endsWith('.jpg')||n.endsWith('.jpeg'))return 'image/jpeg';if(n.endsWith('.png'))return 'image/png';return 'application/octet-stream'}
function triggerDownload(blob){const url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=fileName;a.style.display='none';document.body.appendChild(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),60000)}
async function finishStoredDownload(db){const parts=await loadParts(db),blob=new Blob(parts,{type:mimeForName(fileName)});if(blob.size!==fileSize)throw new Error('Stored file size does not match');triggerDownload(blob);status.textContent='Download ready.';progress.value=1;setTimeout(()=>deleteTransfer(db,transferKey).catch(()=>{}),5*60*1000)}
function waitIceComplete(pc){if(pc.iceGatheringState==='complete')return Promise.resolve();return new Promise((resolve,reject)=>{const timer=setTimeout(()=>{pc.removeEventListener('icegatheringstatechange',check);reject(new Error('Fast connection timed out'))},6000);function check(){if(pc.iceGatheringState==='complete'){clearTimeout(timer);pc.removeEventListener('icegatheringstatechange',check);resolve()}}pc.addEventListener('icegatheringstatechange',check)})}
function formatSpeed(bytes,ms){if(ms<=0)return '0 MB/s';return (bytes/(1024*1024)/(ms/1000)).toFixed(1)+' MB/s'}

async function fastDownload(state){
if(!('RTCPeerConnection' in window)||!('indexedDB' in window))throw new Error('Fast browser transfer is unavailable');
const db=await openDB();cleanupStale(db).catch(()=>{});
let stored=await readState(db);
if(stored.offset===fileSize){state.started=true;await finishStoredDownload(db);return}

const pc=new RTCPeerConnection({iceServers:[]}),dc=pc.createDataChannel('resumexfer-file',{ordered:true});
dc.binaryType='arraybuffer';
let done=false,rejectTransfer,resolveTransfer,current=stored.offset,durable=stored.offset,blockStart=stored.offset,blockIndex=stored.nextIndex,blockParts=[],blockBytes=0,storageChain=Promise.resolve();
const startedAt=performance.now(),startedBytes=stored.offset;let lastUI=0;
const transferDone=new Promise((resolve,reject)=>{resolveTransfer=resolve;rejectTransfer=reject});
function fail(err){if(done)return;done=true;try{dc.close()}catch(e){}try{pc.close()}catch(e){}rejectTransfer(err instanceof Error?err:new Error(String(err)))}
activeFastCancel=()=>fail(new Error('Transfer cancelled'));
function queueBlock(parts,index,start,end){const blob=new Blob(parts,{type:'application/octet-stream'});storageChain=storageChain.then(async()=>{await storeBlock(db,index,start,end,blob);durable=end;if(!done&&dc.readyState==='open')dc.send(JSON.stringify({cmd:'ack',offset:end}))});storageChain.catch(fail)}
function flushBlock(){if(!blockBytes)return;const parts=blockParts,index=blockIndex,start=blockStart,end=current;blockIndex++;blockStart=current;blockParts=[];blockBytes=0;queueBlock(parts,index,start,end)}
function updateUI(force=false){const now=performance.now();if(!force&&now-lastUI<250)return;lastUI=now;progress.value=fileSize?current/fileSize:1;status.textContent='Downloading '+Math.round(progress.value*100)+'% • '+formatSpeed(current-startedBytes,now-startedAt)}
function acceptBinary(bytes){if(done)return;state.started=true;blockParts.push(bytes);blockBytes+=bytes.byteLength;current+=bytes.byteLength;if(current>fileSize){fail(new Error('Received more data than expected'));return}if(blockBytes>=BLOCK||current===fileSize)flushBlock();updateUI()}
dc.onmessage=event=>{if(done)return;if(typeof event.data==='string'){let msg;try{msg=JSON.parse(event.data)}catch(e){fail(e);return}if(msg.type==='meta'){if(msg.name!==fileName||Number(msg.size)!==fileSize||Number(msg.offset)!==stored.offset){fail(new Error('Shared file changed; restart the transfer'));return}state.started=true;status.textContent=stored.offset?'Resuming fast transfer…':'Starting fast transfer…';return}if(msg.type==='error'){fail(new Error(msg.error||'Fast transfer failed'));return}if(msg.type==='complete'){(async()=>{try{if(blockBytes)flushBlock();await storageChain;if(done)return;if(current!==fileSize||durable!==fileSize)throw new Error('Fast transfer ended before browser storage completed');updateUI(true);await finishStoredDownload(db);done=true;try{dc.close()}catch(e){}try{pc.close()}catch(e){}resolveTransfer()}catch(e){fail(e)}})();return}return}if(event.data instanceof ArrayBuffer){acceptBinary(new Uint8Array(event.data));return}event.data.arrayBuffer().then(buf=>acceptBinary(new Uint8Array(buf))).catch(fail)};
dc.onerror=()=>fail(new Error('Fast connection failed'));
dc.onclose=()=>{if(!done)fail(new Error('Fast connection closed'))};
pc.onconnectionstatechange=()=>{if(pc.connectionState==='failed'||pc.connectionState==='closed')fail(new Error('Fast connection failed'))};
const opened=new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(new Error('Fast connection timed out')),8000);dc.onopen=()=>{clearTimeout(timer);resolve()}});
const offer=await pc.createOffer();await pc.setLocalDescription(offer);await waitIceComplete(pc);
const response=await routeFetch('api/fast/offer',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(pc.localDescription)},10000);
if(!response.ok)throw new Error('Fast connection was rejected');
const answer=await response.json();await pc.setRemoteDescription(answer);await opened;
dc.send(JSON.stringify({cmd:'start',index:0,offset:stored.offset}));
return transferDone
}

downloadButton.onclick=async()=>{
if(cancelled)return;downloadButton.disabled=true;const state={started:false};
try{if(fileSize>=LARGE_DIRECT){status.textContent='Starting large-file download…';progress.removeAttribute('value');window.location.assign(fallback.href);return}status.textContent='Preparing fast transfer…';await fastDownload(state)}
catch(e){if(cancelled){status.textContent='Cancelled.'}else if(!state.started){status.textContent='Starting direct download…';window.location.assign(fallback.href)}else{status.textContent='Transfer interrupted. Tap Download to resume.'}}
finally{activeFastCancel=null;downloadButton.disabled=false}
};
}
</script></body></html>`
