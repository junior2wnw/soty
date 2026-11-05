/** Fixed host measurements, never inferred business outcomes or visitor identities. */
export const HOST_METRICS=Object.freeze([
 ['http.requests','Запросы','counter','count'],['http.errors','Ошибки запросов','counter','count'],['http.duration','Время ответа','distribution','ms'],
 ['process.rss','Память процесса','gauge','bytes'],['process.cpu','CPU процесса','gauge','percent'],['process.uptime','Время работы процесса','gauge','seconds'],
 ['live.scopes','Подключения Live','gauge','count'],['live.waiters','Ожидания Live','gauge','count'],
 ['calls.active','Активные разговоры','gauge','count'],['calls.sockets','Сокеты разговоров','gauge','count'],['calls.pending','Ожидания сигнализации','gauge','count'],
 ['push.dropped','Пропуски Push','counter','count'],['push.failures','Ошибки адресатов Push','counter','count'],
 ['inference.requests','Запросы модели','counter','count'],['inference.errors','Ошибки модели','counter','count'],['inference.duration','Время ответа модели','distribution','ms'],['inference.firstoutput','Первый ответ модели','distribution','ms'],
 ['storage.available','Хранилище доступно','gauge','boolean'],['connector.online','Коннекторы онлайн','gauge','count'],
 ['capabilities.succeeded','Успешные вызовы агента','counter','count'],['capabilities.failed','Ошибки вызовов агента','counter','count'],
].map(([id,label,kind,unit])=>Object.freeze({id,label,kind,unit})));
export const MONITORING_OPERATIONS=Object.freeze(['monitoring.describe','monitoring.query','monitoring.config.put','monitoring.metric.put','monitoring.metric.record','monitoring.audit.list','monitoring.admin.invoke']);
export const MONITORING_RIGHTS=Object.freeze({read:'monitoring.read',configure:'monitoring.configure',admin:'monitoring.admin',record:'monitoring.record'});
