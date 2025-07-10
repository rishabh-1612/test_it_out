from typing import Dict, List, Any
from logging import getLogger
from typing import Optional
from helper_func import project_exec_call
from logger import info_logger, error_logger, warning_logger  # noqa: F401
from analyser_repo_pipeline.pipeline_helpers import ThreadWithReturnValue


def process_batch_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not all(key in entry for key in ['collection_id', 'batch_id']):
            raise KeyError(f"Missing required keys in entry: {entry}")
        
        collection_id = entry['collection_id']
        batch_id = entry['batch_id']
        batch_data = entry.get('batch_data', [])
        
        if not batch_data:
            raise ValueError("File info list cannot be empty")
        
        file_info = []
        for file_entry in batch_data:
            try:
                file_summary = file_entry.get('file_summary', '')
                file_path = file_entry.get('file_path', '')
                
                markdown_content = format_file_info(file_summary, file_path)
                file_info.append(markdown_content)
            except Exception as e:
                error_logger.error(f"Error processing file entry {file_entry}: {str(e)}")
        
        new_summary = process_file_info(file_info)
        
        updated_entry = entry.copy()
        updated_entry['batch_summary'] = new_summary
        return updated_entry
    except ValueError as e:
        error_logger.error(f"Error processing entry {entry}: {str(e)}")
        return None  
    except Exception as e:
        error_logger.error(f"Error processing entry {entry}: {str(e)}")
        error_entry = entry.copy()
        error_entry['error'] = str(e)
        error_entry['batch_summary'] = "Error occurred during processing"
        return error_entry

def process_batch_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not data:
        error_logger.error("No data to process")
        return []
    
    threads = []
    results = []
    
    for entry in data:
        thread = ThreadWithReturnValue(target=process_batch_entry, args=(entry,))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        result = thread.join()
        if result is not None: 
            results.append(result)
    
    return results




def format_file_info(summary: str, path: str) -> str:
    """
    Format file information in markdown.
    
    Args:
        summary: File summary text
        path: File path
    
    Returns:
        Markdown formatted string
    
    Raises:
        ValueError: If path is empty or None
    """
    try:
        if not path:
            raise ValueError("File path cannot be empty")
            
        markdown = f"## File Information\n\n"
        markdown += f"### Path\n{path}\n\n"
        markdown += f"### Summary\n{summary or 'No summary available'}\n\n"
        markdown += "---\n"
        return markdown
        
    except Exception as e:
        error_logger.error(f"Error formatting file info - Path: {path}, Summary: {summary}: {str(e)}")
        raise

def process_file_info(file_info: List[str]) -> str:
    """
    Process the formatted file information (Function B).
    
    Args:
        file_info: List of markdown formatted strings containing file information
    
    Returns:
        String containing the new batch summary
    
    Raises:
        ValueError: If file_info is None or empty
    """
    try:
        if not file_info:
            raise ValueError("File info list cannot be empty")
            
        batch_summary, status_code = project_exec_call(file_info)
        return batch_summary

        
    except Exception as e:
        error_logger.error(f"Error processing file info: {str(e)}")
        raise