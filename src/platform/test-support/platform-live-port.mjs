// Identity/voice adapter fixtures declare the independent live-feed owner.
// The real wait/refresh/cursor behavior is exercised in platform-live-client tests.
export function createPlatformLiveTestPort(records=[]){
  return{createPlatformLiveClient(options){const record={options,disposals:0,reconnects:0};records.push(record);return{
    dispose(){record.disposals++;},reconnect(){record.reconnects++;},
  };}};
}
