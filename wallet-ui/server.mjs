import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const publicDir=fileURLToPath(new URL('./public/',import.meta.url));
const files=new Set(['index.html','app.js','demo.js','styles.css','scenarios.json']);
const mime={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json; charset=utf-8'};

export function createUiServer(opsUrl='http://127.0.0.1:8082') {
 const target=new URL(opsUrl);
 if(target.protocol!=='http:')throw Error('OPS_URL must use http');
 return http.createServer(async(req,res)=>{
  const url=new URL(req.url,'http://localhost');
  if(url.pathname.startsWith('/api/ops/')||url.pathname.startsWith('/api/lab/')) {
   // Same-origin bridge only; business/query/scenario logic lives outside the frontend.
   const upstream=http.request({hostname:target.hostname,port:target.port||80,path:url.pathname+url.search,method:req.method,headers:{'content-type':req.headers['content-type']||'application/json'}},response=>{
    res.writeHead(response.statusCode,{'content-type':response.headers['content-type']||'application/json','cache-control':'no-store'});response.pipe(res);
   });
   upstream.setTimeout(10000,()=>upstream.destroy(Error('Ops API timeout')));
   upstream.on('error',()=>{if(!res.headersSent){res.writeHead(502,{'content-type':'application/json; charset=utf-8'});res.end(JSON.stringify({code:'OPS_UNAVAILABLE',message:'Ops API chưa chạy. Bật logging-service ở cổng 8082 hoặc xem dữ liệu minh họa.'}));}else res.destroy();});
   req.pipe(upstream);return;
  }
  if(req.method!=='GET'&&req.method!=='HEAD'){res.writeHead(405);res.end();return;}
  const file=url.pathname==='/'?'index.html':url.pathname.slice(1);
  if(!files.has(file)){res.writeHead(404);res.end('Not found');return;}
  try{const bytes=await readFile(path.join(publicDir,file));res.writeHead(200,{'content-type':mime[path.extname(file)],'cache-control':'no-store'});res.end(req.method==='HEAD'?undefined:bytes);}catch{res.writeHead(404);res.end('Not found');}
 });
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
 const port=Number(process.env.UI_PORT||3000);
 const server=createUiServer(process.env.OPS_URL);
 server.on('error',error=>{console.error(error.code==='EADDRINUSE'?`Cổng ${port} đang được dùng. Dừng bản UI cũ hoặc đặt UI_PORT khác.`:error.message);process.exitCode=1;});
 server.listen(port,'127.0.0.1',()=>console.log(`Wallet UI: http://localhost:${port} (Ctrl+C để dừng)`));
}
