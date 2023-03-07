- [Cutting an MCAP](#cutting-an-mcap)
  - [Split on time](#split-on-time)
  - [Split on topic](#split-on-topic)
- [Converting](#converting)
  - [Topic](#topic)
    - [Rename a topic](#rename-a-topic)
    - [Remove a topic](#remove-a-topic)
    - [Change ROS Timestamp to publish time](#change-ros-timestamp-to-publish-time)
    - [Add a time offset to ROS Timestamp](#add-a-time-offset-to-ros-timestamp)
  - [Pointcloud](#pointcloud)
    - [Remove zero points from PointCloud2](#remove-zero-points-from-pointcloud2)
    - [Rotate a pointcloud](#rotate-a-pointcloud)
    - [Rename PointCloud2 field name](#rename-pointcloud2-field-name)
  - [TF](#tf)
    - [Remove Transform](#remove-transform)
    - [Insert Static Transform](#insert-static-transform)

# Cutting an MCAP

When `keep_tf_tree` is set to `true` all splits will have the same `/tf_static` messages.

## Split on time

> config.yaml

```yaml
keep_tf_tree: true
splits:
  - start:  1676549454.0
    end:    1676549554.0
    name:   beginning
  - start:  1676549554.0
    end:    1676549654.0
    name:   end.mcap
```

`kappe cut --config config.yaml ./input.mcap ./output_folder`

Results in a folder with the following structure:

```bash
output_folder
├── beginning.mcap
└── end.mcap
```

## Split on topic

Splits the mcap file into multiple files, every time a message on the topic `/marker` is read.
The file will be split **before** the message is read.
`debounce` is the time in seconds that the cutter will wait before splitting the file again, default is 0.

```yaml
keep_tf_tree: true
split_on_topic:
  topic: "/marker"
  debounce: 10

```

# Converting

`kappe convert [-h] [--config CONFIG] [--overwrite] input output`

- `input` Can be a folder or a file.

## Topic

### Rename a topic

```yaml
topic:
  mapping:
    /points: /sensor/points
```

### Remove a topic

```yaml
topic:
  remove:
    - /points
```

### Change ROS Timestamp to publish time

Change the time of the ROS Timestamp to the time the message was published.

```yaml
time_offset:
  /sensor/points:
    pub_time: True
```

### Add a time offset to ROS Timestamp

Add 15 seconds to the ROS Timestamp.

```yaml
time_offset:
  /sensor/points:
    sec: 15
    nanosec: 0
```

## Pointcloud

### Remove zero points from PointCloud2

```yaml
point_cloud:
  /sensor/points:
    remove_zero: true
```

### Rotate a pointcloud

```yaml
point_cloud:
  /sensor/points:
    rotation:
       euler_deg: [180, 0, 0]
```

### Rename PointCloud2 field name

Changes the field name of the PointCloud2 message, from `AzimuthAngle` to `azimuth_angle`.

```yaml
point_cloud:
  /sensor/points:
    field_mapping:
      AzimuthAngle: azimuth_angle
```

## TF

> To update a static transform you need to remove the old one and insert a new one.

### Remove Transform

Removes all transform where the child_frame_id is `test_data_frame` or `other_frame`.

```yaml
tf_static:
  remove:
    - test_data_frame
    - other_frame
```

### Insert Static Transform

> Rotation can be specified in `euler_deg` or `quaternion`

```yaml
tf_static:
  insert:
    - frame_id: base
      child_frame_id: also_base

    - frame_id: base
      child_frame_id: sensor
      translation:
        x: -0.1
        y: 0
        z: 0.1
      rotation:
        euler_deg: [0, 90, 0]
```
