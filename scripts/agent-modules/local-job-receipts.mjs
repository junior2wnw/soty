// Only the existing connector executes jobs. This journal records its original
// process result; recovery repeats result delivery, never a process or prompt.
export function createLocalJobReceipts(api, { root, binding }) {
  const schema = 'soty.local-job-receipt.v1', bindingDigest = api.digest(JSON.stringify(binding));
  const validId = id => typeof id === 'string' && /^job_[a-f0-9]{32}$/u.test(id);
  const filename = (id, suffix) => { if (!validId(id)) throw new Error('local_receipt_invalid_job'); return api.join(root, `${id}.${suffix}.json`); };
  const decode = async (id, suffix) => {
    const value = await api.readJson(filename(id, suffix));
    if (value.schema !== schema || value.jobId !== id || value.bindingDigest !== bindingDigest
      || !/^[a-f0-9]{64}$/u.test(value.jobDigest)) throw new Error('local_receipt_binding_denied');
    if (suffix === 'result' && (value.resultDigest !== api.digest(JSON.stringify(value.result))
      || typeof value.result?.ok !== 'boolean' || !Number.isInteger(value.result.exitCode)
      || typeof value.result.text !== 'string' || value.result.text.length > 1000000)) throw new Error('local_receipt_corrupt');
    return value;
  };
  const missing = error => error?.code === 'ENOENT';
  return {
    async claim(job) {
      const id = job?.id, target = filename(id, 'started');
      await api.mkdir(root);
      if ((await api.list(root)).length >= 6000) throw new Error('local_receipt_capacity');
      try { await api.writeExclusive(target, { schema, jobId: id, bindingDigest, jobDigest: api.digest(JSON.stringify(job)) }); return true; }
      catch (error) { if (error?.code !== 'EEXIST') throw error; await decode(id, 'started'); return false; }
    },
    async finish(id, result) {
      const start = await decode(id, 'started'), value = { ...start, result, resultDigest: api.digest(JSON.stringify(result)) };
      try { await api.writeExclusive(filename(id, 'result'), value); }
      catch (error) {
        if (error?.code !== 'EEXIST') throw error;
        if ((await decode(id, 'result')).resultDigest !== value.resultDigest) throw new Error('local_receipt_result_conflict');
      }
      return value;
    },
    async acknowledge(id) {
      const value = await decode(id, 'result');
      try { await api.writeExclusive(filename(id, 'ack'), { ...value, result: undefined }); }
      catch (error) { if (error?.code !== 'EEXIST') throw error; await decode(id, 'ack'); }
    },
    async pending() {
      let files; try { files = await api.list(root); } catch (error) { if (missing(error)) return []; throw error; }
      if (files.length > 6000) throw new Error('local_receipt_capacity');
      const values = [];
      for (const name of files) {
        const match = /^(job_[a-f0-9]{32})\.result\.json$/u.exec(name); if (!match) continue;
        const id = match[1];
        try { await decode(id, 'ack'); continue; } catch (error) { if (!missing(error)) throw error; }
        const start = await decode(id, 'started'), value = await decode(id, 'result');
        if (start.jobDigest !== value.jobDigest) throw new Error('local_receipt_job_conflict');
        values.push(value);
      }
      return values;
    },
  };
}
