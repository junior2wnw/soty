# Root release candidate: same-app short sessions

Release branch `codex/root-short-session-release-20261013` combines
`a08c068ccca6f79c8889d9a3dffb530ffc310485` with the five qualified Host files
from `51698045c2cfda21296aafafc070494a23bd05ae` (cherry-pick `87be36a7`).
Native UI consumer remains `a87e4c64dcd386c4c1dc5fa988fad7d54c77981f`.

The runtime candidate is assembled from this new frozen canonical source and
its newly qualified pretest stage, using the unchanged production Docker tail.
The prior 06ff stage/image is evidence of the build procedure only; its gate
receipts cannot qualify this source. SourceOFF, Native format1 and owner-pending
remain deployment conditions. No Live/config/grant/owner operation is part of
this candidate preparation.

Required release gates: normal UI build, source/blob and runtime/package pins,
all normal Dockerfile qualification commands including the existing World
Runner, Host rotation tests, targeted same-app missing/forged/private-ACK and
scope/revoke guards, and an isolated real browser test for both Native roles,
same mounted frame, short expiry and one unknown write.

Fresh encrypted current-state checkpoint and restore/current-config agreement
are mandatory before the parent's stock rollout. The earlier Root backup
`96a458...` (04:01 before six-tool catalog) is historical evidence and does not
serve as this candidate's fresh beforeimage. Qualified image pins, gate
receipts and exact handoff are written to the task's output directory once
verified. Pending gates must remain explicit; Source/code/browser/runtime,
backup/restore and Live acceptance are separate facts.
