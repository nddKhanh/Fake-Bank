// Presentation fixtures only. Never used as a fallback for a failed API request.
const ids = ['11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222','33333333-3333-4333-8333-333333333333'];
const accountIds = ['aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'];
const time = n => `2026-10-09T07:${String(n).padStart(2,'0')}:00Z`;
const transfers = [
 {id:ids[0],type:'OUTBOUND',status:'UNKNOWN',amount:'20000',currency:'VND',fromAccountId:accountIds[0],toAccountId:null,idempotencyKey:'demo-lost-response',source:'ORDER',sourceId:'ORD-1024',bankRequestId:'br-demo-1024',failureCode:'READ_TIMEOUT',createdAt:time(10),updatedAt:time(11)},
 {id:ids[1],type:'INTERNAL',status:'COMPLETED',amount:'30000',currency:'VND',fromAccountId:accountIds[0],toAccountId:accountIds[1],idempotencyKey:'demo-payment-1023',source:'ORDER',sourceId:'ORD-1023',bankRequestId:null,failureCode:null,createdAt:time(4),updatedAt:time(4)},
 {id:ids[2],type:'INTERNAL',status:'FAILED',amount:'80000',currency:'VND',fromAccountId:accountIds[0],toAccountId:accountIds[1],idempotencyKey:'demo-insufficient',source:null,sourceId:null,bankRequestId:null,failureCode:'INSUFFICIENT_FUNDS',createdAt:time(12),updatedAt:time(12)}
];
const accounts = [
 {id:accountIds[0],accountNumber:'W-A',ownerRef:'user-A',type:'USER',cachedBalance:'50500',ledgerBalance:'50000',diff:'500',entryCount:3},
 {id:accountIds[1],accountNumber:'W-B',ownerRef:'user-B',type:'USER',cachedBalance:'30000',ledgerBalance:'30000',diff:'0',entryCount:1},
 {id:'cccccccc-cccc-4ccc-8ccc-cccccccccccc',accountNumber:'CLEARING',ownerRef:'system',type:'CLEARING',cachedBalance:'20000',ledgerBalance:'20000',diff:'0',entryCount:1},
 {id:'dddddddd-dddd-4ddd-8ddd-dddddddddddd',accountNumber:'SYSTEM_BANK',ownerRef:'system',type:'SYSTEM_BANK',cachedBalance:'-100000',ledgerBalance:'-100000',diff:'0',entryCount:1}
];
const names=['Tổng debit = tổng credit toàn hệ thống','Mỗi bút toán đều cân','Không có bút toán rỗng','Không tài khoản USER nào âm','Số dư lưu sẵn khớp với sổ cái','Tổng số dư mọi tài khoản = 0','Transfer nội bộ COMPLETED đều có bút toán','Thứ tự bút toán từng tài khoản không bị hổng'];
const checks=names.map((checkName,i)=>({sortOrder:i+1,checkName,violations:[4,5].includes(i)?1:0,drilldown:i===4||i===5?'#accounts?mismatchedOnly=true':'#schema'}));
const metrics=[
 ['open','Transfer đang dở',1,'WARN','#queues'],['unknown','Chưa biết kết quả · UNKNOWN',1,'WARN','#transfers?status=UNKNOWN'],
 ['review','Cần người xem · PENDING_REVIEW',0,'OK','#transfers?status=PENDING_REVIEW'],['outbox','Outbox chưa gửi',1,'WARN','#queues'],
 ['callbacks','Callback chưa xử lý',0,'OK','#queues'],['signature','Callback chữ ký sai',1,'FAIL','#queues'],['issues','Sai lệch đối soát chưa xử lý',1,'FAIL','#reconciliation']
].map(([key,label,value,level,drilldown])=>({key,label,value,level,drilldown}));
const overview={checks,metrics,buckets:transfers.map(t=>({minute:t.createdAt,status:t.status,count:1})),p50:'0.12 giây',p95:'0.31 giây',observedAt:time(13)};
const statement=[
 {accountSeq:1,createdAt:time(0),ledgerType:'TOPUP',direction:'CREDIT',amount:'100000',balanceAfter:'100000',recomputedBalance:'100000',transferId:null},
 {accountSeq:2,createdAt:time(4),ledgerType:'PRINCIPAL',direction:'DEBIT',amount:'30000',balanceAfter:'70000',recomputedBalance:'70000',transferId:ids[1]},
 {accountSeq:3,createdAt:time(10),ledgerType:'TO_CLEARING',direction:'DEBIT',amount:'20000',balanceAfter:'50000',recomputedBalance:'50000',transferId:ids[0]}
];
const chaosRules=[['slow','DELAY'],['five-xx','ERROR_5XX'],['lose-response','LOSE_RESPONSE'],['callback-flip','CALLBACK_FAIL_AFTER_SUCCESS'],['callback-dup','CALLBACK_DUPLICATE'],['callback-late','CALLBACK_LATE'],['down','DOWNTIME'],['reject','REJECT_ACCOUNT']].map(([name,mode])=>({name,mode,enabled:name==='lose-response',probability:'1.000',params:{}}));
const queues={openTransfers:[transfers[0]],outbox:[{id:'1',transferId:ids[0],eventType:'transfer.created',status:'PENDING',attempts:2,createdAt:time(10)}],callbacks:[{id:'2',transferId:ids[0],eventType:'transfer.success',status:'IGNORED',attempts:0,createdAt:time(11),signatureValid:false}],chaosRules,transport:'Kafka (minh họa)'};
const reconciliation={issues:[{id:'1',issueType:'BALANCE_CACHE_MISMATCH',status:'OPEN',accountId:accountIds[0],transferId:null,expectedAmount:'50000',actualAmount:'50500',resolutionNote:'Dữ liệu minh họa: cache bị sửa thêm 500 VND. Chưa có bút toán điều chỉnh.',adjustmentId:null,createdAt:time(13)}],runs:[{id:'1',createdAt:time(13),issueCount:1}]};
const run={id:'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee',scenarioId:'B1',seed:42,status:'FAIL',startedAt:time(10),finishedAt:time(13),phase:'COMPARE',converged:true,stablePolls:3,before:{balances:{USER:'100000',CLEARING:'0',SYSTEM_BANK:'-100000'},transfers:{COMPLETED:0,UNKNOWN:0}},after:{balances:{USER:'80500',CLEARING:'20000',SYSTEM_BANK:'-100000'},transfers:{COMPLETED:1,UNKNOWN:0}},checks,assertions:[{label:'Bank ghi đúng một giao dịch cho client_request_id',passed:true,actual:'1',expected:'1',transferId:ids[0]},{label:'Số dư cache khớp sổ cái',passed:false,actual:'50500',expected:'50000',transferId:ids[0]}],transferIds:[ids[0]],error:null};
const page=(items,p)=>{const size=Number(p.get('size')||50),n=Number(p.get('page')||0);return {items:items.slice(n*size,(n+1)*size),total:items.length,page:n,size};};
export function demoGet(url) {
 const [path,query='']=url.split('?'),p=new URLSearchParams(query);
 if(path==='/capabilities')return {backendImplemented:false,labEnabled:false,dataSource:'DEMO'};
 if(path==='/overview')return overview;
 if(path==='/transfers')return page(transfers.filter(t=>(!p.get('status')||t.status===p.get('status'))&&(!p.get('type')||t.type===p.get('type'))&&(!p.get('search')||[t.id,t.idempotencyKey,t.sourceId].some(v=>v?.toLowerCase().includes(p.get('search').toLowerCase())))&&(!p.get('from')||t.createdAt>=p.get('from'))&&(!p.get('to')||t.createdAt<=p.get('to'))),p);
 if(path.startsWith('/transfers/')) {
  const transfer=transfers.find(t=>t.id===path.split('/')[2]); if(!transfer)throw Error('Không tìm thấy giao dịch minh họa.');
  const bank=transfer.type==='OUTBOUND';
  return {transfer,timeline:[{at:transfer.createdAt,source:'TRẠNG THÁI',what:'(mới) → CREATED',detail:'API'},...(transfer.status==='FAILED'?[{at:transfer.updatedAt,source:'TRẠNG THÁI',what:'CREATED → FAILED',detail:'INSUFFICIENT_FUNDS'}]:[{at:transfer.createdAt,source:'SỔ CÁI',what:bank?'TO_CLEARING':'PRINCIPAL',detail:bank?'W-A DEBIT 20.000 · CLEARING CREDIT 20.000':'W-A DEBIT 30.000 · W-B CREDIT 30.000'},{at:transfer.createdAt,source:'TRẠNG THÁI',what:bank?'CREATED → FUNDS_RESERVED':'CREATED → COMPLETED',detail:'WORKER'}]),...(bank?[{at:time(10),source:'KAFKA (OUTBOX)',what:'transfer.created',detail:'Chưa gửi · đã thử 2 lần'},{at:time(11),source:'GỌI BANK',what:'TRANSFER #1 → TIMEOUT',detail:'READ_TIMEOUT · cùng bank_request_id br-demo-1024'},{at:time(11),source:'TRẠNG THÁI',what:'SENT_TO_BANK → UNKNOWN',detail:'Chờ inquiry; chưa được phép hoàn tiền'}]:[])],postings:transfer.status==='FAILED'?[]:[{id:'p1',type:bank?'TO_CLEARING':'PRINCIPAL',accountNumber:'W-A',direction:'DEBIT',amount:transfer.amount,balanceAfter:bank?'50000':'70000'},{id:'p1',type:bank?'TO_CLEARING':'PRINCIPAL',accountNumber:bank?'CLEARING':'W-B',direction:'CREDIT',amount:transfer.amount,balanceAfter:bank?'20000':'30000'}],attempts:bank?[{attemptNo:1,operation:'TRANSFER',bankRequestId:'br-demo-1024',bankReference:null,status:'TIMEOUT',httpStatus:null,errorCode:'READ_TIMEOUT',sentAt:time(10),respondedAt:time(11)}]:[]};
 }
 if(path==='/accounts')return page(accounts.filter(a=>(p.get('mismatchedOnly')!=='true'||a.diff!=='0')&&(!p.get('search')||[a.accountNumber,a.ownerRef,a.id].some(v=>v.toLowerCase().includes(p.get('search').toLowerCase())))),p);
 if(path.startsWith('/accounts/')) {const account=accounts.find(a=>a.id===path.split('/')[2]);if(!account)throw Error('Không tìm thấy tài khoản minh họa.');return {account,statement:account.id===accountIds[0]?statement:account.id===accountIds[1]?[{...statement[1],accountSeq:1,direction:'CREDIT',balanceAfter:'30000',recomputedBalance:'30000'}]:[{accountSeq:1,createdAt:time(0),ledgerType:account.type==='CLEARING'?'TO_CLEARING':'TOPUP',direction:account.type==='CLEARING'?'CREDIT':'DEBIT',amount:account.type==='CLEARING'?'20000':'100000',balanceAfter:account.ledgerBalance,recomputedBalance:account.ledgerBalance,transferId:account.type==='CLEARING'?ids[0]:null}]};}
 if(path==='/queues')return queues;
 if(path==='/reconciliation')return {...reconciliation,issues:reconciliation.issues.filter(i=>!p.get('status')||i.status===p.get('status'))};
 if(path==='/runs')return page([run],p);
 if(path.startsWith('/runs/')){if(path.split('/')[2]!==run.id)throw Error('Không tìm thấy lần chạy minh họa.');return run;}
 throw Error('Chưa có dữ liệu minh họa cho màn này.');
}
