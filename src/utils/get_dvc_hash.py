import hashlib
import os
import logging

logger = logging.getLogger(__name__)

def get_dvc_hash(file_path: str, chunk_size: int = 8192) -> str:
    """
    Computes the MD5 hash of a file (exactly as DVC does for local files).
    
    This function is memory-efficient and safe for large datasets because it reads the 
    file in chunks.
    
    Args:
        file_path (str): The absolute or relative path to the data file.
        chunk_size (int): The number of bytes to read into memory at a time.
                          Default is 8192 bytes (8KB).
                          
    Returns:
        str: The 32-character MD5 hex digest matching the DVC lock file.
        
    Raises:
        FileNotFoundError: If the file does not exist.
        IsADirectoryError: If the path points to a directory instead of a file.
    """
    if not os.path.exists(file_path):
        logger.error(f"Traceability Error: File not found at {file_path}")
        raise FileNotFoundError(f"Cannot compute hash. File not found: {file_path}")
        
    if os.path.isdir(file_path):
        logger.error(f"Traceability Error: Attempted to hash a directory ({file_path})")
        raise IsADirectoryError(
            f"Path '{file_path}' is a directory. DVC hashes directories using "
            "a custom JSON tree structure. Please pass individual file paths."
        )

    md5_hash = hashlib.md5()
    
    try:
        with open(file_path, "rb") as f:
            # Read the file in chunks to prevent MemoryErrors on large datasets
            for chunk in iter(lambda: f.read(chunk_size), b""):
                md5_hash.update(chunk)
                
        calculated_hash = md5_hash.hexdigest()
        logger.info(f"Successfully computed DVC hash for {file_path}: {calculated_hash}")
        return calculated_hash
        
    except PermissionError:
        logger.error(f"Traceability Error: Permission denied when reading {file_path}")
        raise
    except Exception as e:
        logger.error(f"Traceability Error: Unexpected error hashing {file_path} - {str(e)}")
        raise