// Frozen independent legacy oracle; this does not import current writer/DDL.
import {readOrdinaryFormat2} from '../examples/ordinary-app/reader2.mjs';
const [path,realm,...extra]=process.argv.slice(2);
try{if(!path||!realm||extra.length)throw Error('usage');
  const result=readOrdinaryFormat2(path,realm);console.log(JSON.stringify({supported:true,...result}));
}catch{console.log(JSON.stringify({supported:false,code:'ordinary_source_reader_refused'}));process.exitCode=78;}
