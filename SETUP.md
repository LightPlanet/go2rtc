### Create/Update source  
```shell
PUT /api/streams?name=cam_usb&src=ffmpeg:device?video=/dev/video0&input_format=mjpeg#video=h264
```
### Delete source  
```shell
DELETE /api/streams?src=cam_usb
```

### Start/stop saving video  
```shell
POST /api/start-save?name=cam_usb&output-dir=/my_video&duration=45s
POST /api/stop-save?name=cam_usb
```
