import { observeRootState } from './root-state-observer.mjs';
try {
  const value=await observeRootState({source:'/source',ramRoot:'/ram'});
  process.stdout.write(JSON.stringify(value)+'\n');
} catch {
  process.stdout.write('{"ok":false,"code":"root_observation_failed"}\n');
  process.exitCode=1;
}
