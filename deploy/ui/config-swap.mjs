// Maintained form of the accepted core-ui-swap helper. No DATA or service stop.
import fs from 'node:fs';
import path from 'node:path';
import { createHash, createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';

export const sha256 = value => createHash('sha256').update(value).digest('hex');
const need = (value, code) => { if (!value) throw Object.assign(new Error(code), { code }); };
const hex = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const keyValid = key => Buffer.isBuffer(key) && key.length === 32;
const purposeValid = purpose => ['baseline', 'candidate'].includes(purpose);
export function fsyncDirectory(directory) {
  if (process.platform === 'win32') return; // Host qualification runs on Linux.
  const fd = fs.openSync(directory, 'r'); try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
}

export function encryptConfig(plain, key, purpose) {
  need(keyValid(key) && purposeValid(purpose) && Buffer.isBuffer(plain) && plain.length > 0 && plain.length <= 131072, 'encrypted_config_invalid');
  const nonce = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, nonce);
  cipher.setAAD(Buffer.from('soty.ui.config.v1:' + purpose));
  const ciphertext = Buffer.concat([cipher.update(plain), cipher.final()]);
  return Buffer.from(JSON.stringify({ schema: 'soty.ui.config.v1', purpose,
    nonce: nonce.toString('base64'), tag: cipher.getAuthTag().toString('base64'), ciphertext: ciphertext.toString('base64') }));
}

export function decryptConfig(raw, key, purpose, expectedSha256) {
  need(keyValid(key) && purposeValid(purpose) && hex(expectedSha256) && Buffer.isBuffer(raw) && raw.length <= 262144, 'encrypted_config_invalid');
  try {
    const value = JSON.parse(raw);
    need(value?.schema === 'soty.ui.config.v1' && value.purpose === purpose
      && Object.keys(value).sort().join(',') === 'ciphertext,nonce,purpose,schema,tag', 'encrypted_config_invalid');
    const decode = text => { need(typeof text === 'string', 'encrypted_config_invalid'); const bytes = Buffer.from(text, 'base64'); need(bytes.toString('base64') === text, 'encrypted_config_invalid'); return bytes; };
    const nonce = decode(value.nonce), tag = decode(value.tag), ciphertext = decode(value.ciphertext);
    need(nonce.length === 12 && tag.length === 16 && ciphertext.length <= 131072, 'encrypted_config_invalid');
    const decipher = createDecipheriv('aes-256-gcm', key, nonce);
    decipher.setAAD(Buffer.from('soty.ui.config.v1:' + purpose)); decipher.setAuthTag(tag);
    const plain = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
    if (plain.length === 0 || sha256(plain) !== expectedSha256) { plain.fill(0); need(false, 'decrypted_config_mismatch'); }
    return plain;
  } catch (error) { if (error?.code === 'decrypted_config_mismatch') throw error; need(false, 'encrypted_config_invalid'); }
}

/** Fresh encrypted files only; authenticated readback before returning safe pins. */
export function writeEncryptedConfigs({ directory, baseline, candidate, key }) {
  need(path.isAbsolute(directory) && fs.realpathSync(directory) === directory, 'encrypted_directory_invalid');
  const stat = fs.lstatSync(directory);
  need(stat.isDirectory() && !(stat.mode & 0o077) && stat.uid === process.getuid?.(), 'encrypted_directory_invalid');
  const result = {};
  for (const [purpose, plain] of Object.entries({ baseline, candidate })) {
    const encrypted = encryptConfig(plain, key, purpose), file = path.join(directory, purpose + '.enc');
    const fd = fs.openSync(file, 'wx', 0o600);
    try { fs.writeFileSync(fd, encrypted); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    fsyncDirectory(directory);
    const raw = fs.readFileSync(file), checked = decryptConfig(raw, key, purpose, sha256(plain));
    try { need(checked.equals(plain) && sha256(raw) === sha256(encrypted), 'encrypted_file_changed'); }
    finally { checked.fill(0); }
    result[purpose] = { file, cipherSha256: sha256(raw), plainSha256: sha256(plain) };
  }
  return result;
}

/** One checked atomic change. A failure after rename is ambiguous: never replay. */
export function swapConfig({ filename, expectedSha256, next, nextSha256, transaction, profile, signal, record = () => {} }) {
  need(path.isAbsolute(filename) && fs.realpathSync(path.dirname(filename)) === path.dirname(filename)
    && /^[a-f0-9]{32}$/u.test(transaction) && hex(expectedSha256) && typeof signal === 'function', 'swap_arguments_invalid');
  const parent = fs.lstatSync(path.dirname(filename));
  need(parent.isDirectory() && parent.uid === process.getuid?.() && !(parent.mode & 0o022), 'config_file_invalid');
  need(Buffer.isBuffer(next) && hex(nextSha256) && sha256(next) === nextSha256, 'candidate_bytes_changed');
  const initial = fs.lstatSync(filename);
  const valid = stat => stat.isFile() && !stat.isSymbolicLink() && stat.nlink === 1
    && (!profile || stat.uid === profile.uid && stat.gid === profile.gid && (stat.mode & 0o777) === profile.mode);
  need(valid(initial), 'config_file_invalid');
  need(sha256(fs.readFileSync(filename)) === expectedSha256, 'current_config_changed');
  const temporary = path.join(path.dirname(filename), '.soty-ui-' + transaction + '.tmp');
  let fd, created = false, replaced = false;
  try {
    fd = fs.openSync(temporary, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    created = true;
    fs.writeFileSync(fd, next);
    if (process.platform !== 'win32') fs.fchownSync(fd, initial.uid, initial.gid);
    fs.fchmodSync(fd, initial.mode & 0o777); fs.fsyncSync(fd); fs.closeSync(fd); fd = undefined;
    record('replace-intent');
    const current = fs.lstatSync(filename);
    need(valid(current) && current.dev === initial.dev && current.ino === initial.ino && current.size === initial.size
      && current.mtimeMs === initial.mtimeMs && current.ctimeMs === initial.ctimeMs
      && sha256(fs.readFileSync(filename)) === expectedSha256, 'current_config_changed');
    fs.renameSync(temporary, filename); replaced = true; fsyncDirectory(path.dirname(filename));
    need(valid(fs.lstatSync(filename)) && sha256(fs.readFileSync(filename)) === nextSha256, 'installed_config_changed');
    record('replaced'); signal(); record('signal-sent');
    return { replaced: true, signalSent: true, configSha256: nextSha256, activeReloadVerified: false };
  } catch (error) {
    // Preserve the first failure even when the journal is also unavailable.
    try { record(replaced ? 'reload-unconfirmed' : 'not-applied'); } catch { /* No replay or automatic restore. */ }
    throw Object.assign(error, { replaced, recoveryRequired: replaced });
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
    if (created && !replaced && fs.existsSync(temporary)) fs.unlinkSync(temporary);
  }
}
