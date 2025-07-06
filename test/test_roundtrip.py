import json
from pathlib import Path


from kappe.utils.json_to_mcap import json_to_mcap
from kappe.utils.mcap_to_json import mcap_to_json


def test_pointcloud2_roundtrip_data_integrity(tmp_path: Path):
    """
    Tests that PointCloud2 data is correctly preserved during a full
    JSON -> MCAP -> JSON roundtrip conversion.
    """
    json_file = tmp_path / 'test.jsonl'
    mcap_file = tmp_path / 'test.mcap'
    output_json_file = tmp_path / 'output.jsonl'

    # 1. Create an input JSONL file with a PointCloud2 message
    #    - The point data is in the 'data' field, as produced by mcap_to_json.
    input_json_content = {
        'topic': '/points',
        'log_time': 1672531200000000000,
        'publish_time': 1672531200000000000,
        'sequence': 1,
        'datatype': 'sensor_msgs/msg/PointCloud2',
        'message': {
            'header': {'stamp': {'sec': 1672531200, 'nanosec': 0}, 'frame_id': 'map'},
            'height': 1,
            'width': 2,
            'fields': [
                {'name': 'x', 'offset': 0, 'datatype': 7, 'count': 1},
                {'name': 'y', 'offset': 4, 'datatype': 7, 'count': 1},
                {'name': 'z', 'offset': 8, 'datatype': 7, 'count': 1},
            ],
            'is_bigendian': False,
            'point_step': 12,
            'row_step': 24,
            'points': [{'x': 1.0, 'y': 2.0, 'z': 3.0}, {'x': 4.0, 'y': 5.0, 'z': 6.0}],
            'is_dense': True,
        },
    }
    with json_file.open('w') as f:
        f.write(json.dumps(input_json_content))
        f.write('\n')

    # 2. Convert JSONL to MCAP
    json_to_mcap(mcap_file, json_file)

    # 3. Convert MCAP back to JSONL
    with output_json_file.open('w') as f:
        mcap_to_json(mcap_file, f)

    # 4. Compare the input and output JSONL files
    with json_file.open('r') as f_in, output_json_file.open('r') as f_out:
        input_data = json.load(f_in)
        output_data = json.load(f_out)

        # The core of the test: ensure the point data is identical
        assert input_data['message']['points'] == output_data['message']['points'], (
            'PointCloud2 points should be preserved after roundtrip'
        )

        # Also check other fields to be safe
        assert input_data['topic'] == output_data['topic']
        assert input_data['datatype'] == output_data['datatype']
        assert input_data['message']['header'] == output_data['message']['header']
