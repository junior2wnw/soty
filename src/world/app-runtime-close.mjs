/** The owner supplies its exact runtime/intent fence and owns busy state. */
export async function closeAppRuntime({stage,isCurrent,confirmUnknown,onFailure,commit}) {
  if(!isCurrent())return false;
  try {
    const unknown=stage.hasUnconfirmedWork();
    if(unknown&&!await confirmUnknown(isCurrent))return false;
    if(!isCurrent())return false;
    await stage.flush();
    if(!isCurrent())return false;
    if(stage.hasUnsavedChanges()||!unknown&&stage.hasUnconfirmedWork()){onFailure();return false;}
    commit();return true;
  } catch(error) {
    if(isCurrent())onFailure(error);
    return false;
  }
}
