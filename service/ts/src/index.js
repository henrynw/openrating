import http from 'http';
const s=http.createServer((q,r)=>{if(q.url==='/health'){r.writeHead(200,{'Content-Type':'application/json'});r.end(JSON.stringify({ok:true}))}else{r.writeHead(404);r.end('not found')}});
s.listen(8080,()=>console.log('OpenRating API listening on :8080'))
