#!/usr/bin/env python3
import sys
import traceback

try:
    from taxi_pipeline import taxi_pipeline_rest_api_source, pipeline
    source = taxi_pipeline_rest_api_source()
    load_info = pipeline.run(source, write_disposition="replace")
    print("\nPipeline executed successfully!")
    print(f"Load info: {load_info}")
except Exception as e:
    print(f"\n\nERROR OCCURRED:")
    print(f"Error type: {type(e).__name__}")
    print(f"Error message: {str(e)}")
    
    # Print cause if available
    if hasattr(e, '__cause__') and e.__cause__:
        print(f"\nCaused by: {type(e.__cause__).__name__}: {str(e.__cause__)}")
    
    # Print context if available
    if hasattr(e, '__context__') and e.__context__:
        print(f"\nContext: {type(e.__context__).__name__}: {str(e.__context__)}")
    
    print("\n\nFull traceback:")
    traceback.print_exc()
    sys.exit(1)
