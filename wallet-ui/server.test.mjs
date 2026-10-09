import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { createUiServer } from './server.mjs';
const listen=server=>new Promise(resolve=>server.listen(0,'127.0.0.1',()=>resolve(`http://127.0.0.1:${server.address().port}`)));
test('UI serves assets without backend, rejects traversal and exposes proxy failures',async()=>{
 const upstream=http.createServer();const unused=await listen(upstream);await new Promise(r=>upstream.close(r));
 const server=createUiServer(unused);const base=await listen(server);
 try{
  assert.equal((await fetch(base+'/')).status,200);
  assert.equal((await fetch(base+'/scenarios.json')).status,200);
  assert.equal((await fetch(base+'/%2e%2e/pom.xml')).status,404);
  const result=await fetch(base+'/api/ops/capabilities');assert.equal(result.status,502);assert.equal((await result.json()).code,'OPS_UNAVAILABLE');
 }finally{await new Promise(r=>server.close(r));}
});
test('proxy preserves Ops status and sends Lab JSON to the separate service',async()=>{
 const upstream=http.createServer((req,res)=>{let body='';req.on('data',b=>body+=b);req.on('end',()=>{res.writeHead(501,{'content-type':'application/json'});res.end(JSON.stringify({path:req.url,method:req.method,body}));});});
 const target=await listen(upstream),server=createUiServer(target),base=await listen(server);
 try{const result=await fetch(base+'/api/lab/reset',{method:'POST',headers:{'content-type':'application/json'},body:'{"confirmed":true}'});assert.equal(result.status,501);assert.deepEqual(await result.json(),{path:'/api/lab/reset',method:'POST',body:'{"confirmed":true}'});}
 finally{await new Promise(r=>server.close(r));await new Promise(r=>upstream.close(r));}
});
