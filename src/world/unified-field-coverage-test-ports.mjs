import assert from 'node:assert/strict';
import ts from 'typescript';
import {deriveCanonicalAppCoverage} from './canonical-app-coverage.mjs';

export function coverageTestSource(statements,parsed,printer){
 return ['readAppCoverage','liveAppScene','refreshAppCoverage'].map(name=>{
  const node=statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text===name);assert.ok(node,`Actual Screen ${name} exists`);
  return printer.printNode(ts.EmitHint.Unspecified,node,parsed);
 }).join('\n');
}
export function installCoverageTestPorts(environment){
 if(!Object.hasOwn(environment,'previewCommunity'))environment.previewCommunity=null;
 // The existing mixed Node/VM harness transports JSON; real validation stays strict.
 const clone=value=>JSON.parse(JSON.stringify(value));
 Object.assign(environment,{appProjectionGeneration:0,appCoverageSceneSignature:JSON.stringify({contexts:[],shortcuts:[]}),
  deriveCanonicalAppCoverage:({document,representations})=>deriveCanonicalAppCoverage({document:clone(document),representations:new Map([...representations].map(([id,refs])=>[id,refs===undefined?undefined:clone(refs)]))})});
 return environment;
}
