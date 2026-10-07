import {readOrdinaryFormat3} from '../examples/ordinary-app/reader3.mjs';
const [path,realm,...extra]=process.argv.slice(2);
try{if(!path||!realm||extra.length)throw Error('usage');
  const result=readOrdinaryFormat3(path,realm);console.log(JSON.stringify({supported:true,...result}));
}catch{console.log(JSON.stringify({supported:false,code:'ordinary_source_reader_refused'}));process.exitCode=78;}
