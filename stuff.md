```sh
install -Dm755 target/release/js8d "$HOME/.local/bin/js8d"
install -Dm644 contrib/js8d.service "$HOME/.config/systemd/user/js8d.service"
install -Dm600 contrib/js8d.env "$HOME/.config/js8d/env"
# edit ts
systemctl --user daemon-reload
systemctl --user enable --now js8d
journalctl --user -u js8d
```

ping:
```json
{"op":"ping"}
{"status":"ok"}
```

tx:

```json
{"op":"tx","mode":"normal","dial_hz":14078000,"audio_hz":1500,"message":"CQ CQ DE N0CALL"}
```

status:

```json
{"status":"queued","frames":2}
{"status":"ok"}
```
