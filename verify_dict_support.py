import spatio
import json

def test_dict_support():
    print("Testing direct Python dictionary support...")
    
    # Create in-memory database
    db = spatio.Spatio.memory()
    
    # Test data
    namespace = "test_ns"
    object_id = "obj1"
    point = spatio.Point(10.0, 20.0, 30.0)
    metadata = {
        "id": 123,
        "name": "Test Object",
        "tags": ["a", "b", "c"],
        "active": True,
        "nested": {"x": 1, "y": 2}
    }
    
    print(f"Inserting object with metadata: {metadata}")
    
    # Update location with dict metadata
    db.update_location(namespace, object_id, point, metadata)
    
    # Query back
    print("Querying object...")
    results = db.query_current_within_radius(namespace, point, 100.0)
    
    assert len(results) == 1
    result_id, result_point, result_meta = results[0]
    
    print(f"Received metadata: {result_meta}")
    print(f"Type of received metadata: {type(result_meta)}")
    
    # Verify equality
    # Note: JSON roundtrip might change tuple to list, etc. so we might need loose comparison
    # But pythonize should map dict to dict, list to list.
    assert result_meta == metadata
    assert result_meta["id"] == 123
    assert result_meta["nested"]["x"] == 1
    
    print("SUCCESS: Dictionary support verified!")

if __name__ == "__main__":
    try:
        test_dict_support()
    except Exception as e:
        print(f"FAILURE: {e}")
        exit(1)
