# Synthetic media conformance fixtures

No user media, microphone, account, camera or remote service was used.

`chrome.png`, `chrome.jpg`, `chrome.webp` and `chrome-recorder.webm` were produced by actual Chrome 154 Canvas / AudioContext oscillator / MediaRecorder on 6 October 2026. The manual local generator is `../support/generate-chrome-media-fixtures.mjs`; its exact source and browser metadata are in `chrome-provenance.json`. The WebM is the concatenation of real streaming recorder chunks, including unknown-size Segment and Cluster elements. A requested 350ms wall-clock recording yielded five 60ms Opus packets; the validator derives 300ms rather than trusting the requested timer.

The Ogg/WebM encoder fixtures were generated with the installed FFmpeg/libopus, using synthetic 48kHz mono audio:

```text
ffmpeg -f lavfi -i sine=frequency=440:sample_rate=48000:duration=0.35 -c:a libopus -b:a 32k -ac 1 -vn ffmpeg-opus.ogg
ffmpeg -f lavfi -i sine=frequency=440:sample_rate=48000:duration=2 -c:a libopus -b:a 32k -ac 1 -vn ffmpeg-opus-2s.webm
ffmpeg -f lavfi -i anullsrc=sample_rate=48000:channel_layout=mono -t 121 -c:a libopus -b:a 16k -vn ffmpeg-opus-121s.ogg
```

The 121-second silent Ogg is encoder-valid and smaller than the byte cap; it tests the independent duration bound. Corrupted variants are created separately in negative tests, never used as evidence of supported valid codecs. FFmpeg and Chrome are fixture-generation tools only, not runtime/test-suite dependencies; the validator and tests use Node builtins.

The first upload profile accepts static PNG, baseline/progressive JPEG and static WebP containers with bounded dimensions; PNG additionally checks bounded inflate output/filter rows. WebM accepts one unencrypted Opus track, streaming clusters and non-laced SimpleBlock/BlockGroup packets. Ogg accepts one complete checksummed Opus stream with consecutive pages, packet framing and consistent granules/preSkip. Unsupported animation, lacing, codec mapping or unprovable duration is rejected explicitly. JPEG/WebP/Opus compressed payloads are not fully decoded or certified by these checks. Downstream rendering/decoding still needs its own bounded runtime.

Primary parsing references: [PNG](https://www.w3.org/TR/png-3/), [WebP container](https://developers.google.com/speed/webp/docs/riff_container), [Matroska elements](https://www.matroska.org/technical/elements.html), [Opus packet framing](https://www.rfc-editor.org/rfc/rfc6716.html#section-3), [Ogg Opus granules](https://www.rfc-editor.org/rfc/rfc7845.html#section-4), [Ogg pages/CRC](https://www.rfc-editor.org/rfc/rfc3533.html).
