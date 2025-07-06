import pytest

from kappe.utils.types import ClassDict


def test_classdict_creation():
    """Test ClassDict creation with initial data."""
    data = {'a': 1, 'b': 2, 'c': 3}
    cd = ClassDict(data)
    assert cd == data
    assert len(cd) == 3


def test_classdict_attribute_access():
    """Test attribute access to dict items."""
    cd = ClassDict({'name': 'test', 'value': 42})

    # Test __getattr__
    assert cd.name == 'test'
    assert cd.value == 42

    # Test __setattr__
    cd.new_attr = 'new_value'
    assert cd['new_attr'] == 'new_value'
    assert cd.new_attr == 'new_value'

    # Test __delattr__
    del cd.name
    assert 'name' not in cd
    with pytest.raises(KeyError):
        _ = cd.name


def test_classdict_dict_operations():
    """Test that ClassDict works like a regular dict."""
    cd = ClassDict()
    cd['key1'] = 'value1'
    cd['key2'] = 'value2'

    assert cd['key1'] == 'value1'
    assert cd['key2'] == 'value2'
    assert len(cd) == 2

    # Test iteration
    keys = list(cd.keys())
    assert 'key1' in keys
    assert 'key2' in keys


def test_classdict_mixed_access():
    """Test mixing attribute and dict access."""
    cd = ClassDict()
    cd.attr_key = 'attr_value'
    cd['dict_key'] = 'dict_value'

    assert cd.attr_key == 'attr_value'
    assert cd['attr_key'] == 'attr_value'
    assert cd.dict_key == 'dict_value'
    assert cd['dict_key'] == 'dict_value'
