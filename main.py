
import os
import utils
import sending_email

def process_folder(folder_path: str):

    processed_files = []
    errors = []
    
    try:
        files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        
        for file in files:
            try:

                file_path = os.path.join(folder_path, file)
                data = utils.read_json_file(file_path)
                

                new_filename = f"processed_{timestamp}_{file}"
                new_file_path = os.path.join(folder_path, new_filename)
                
                modifications = []
                
                if data:
                    if 'status' in data and data['status'] == 'pending':
                        data['status'] = 'processed'
                        modifications.append("Updated status from 'pending' to 'processed'")
                    
                    # Add more data transformations as needed
                
                # Rename file
                os.rename(file_path, new_file_path)
                
                # Record processing details
                file_info = {
                    'original_name': file,
                    'new_name': new_filename,
                    'success': True,
                    'status': 'Processed successfully',
                    'modifications': '; '.join(modifications) if modifications else 'No modifications needed',
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                processed_files.append(file_info)
                
            except Exception as e:
                errors.append(f"Error processing {file}: {str(e)}")
                processed_files.append({
                    'original_name': file,
                    'new_name': 'N/A',
                    'success': False,
                    'status': f'Failed: {str(e)}',
                    'modifications': 'N/A',
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                
    except Exception as e:
        errors.append(f"Error accessing folder {folder_path}: {str(e)}")
    
    return processed_files, errors

def main():
    try:
        # Get environment variables
        env_vars = get_env_variables()
        
        # Process folder
        folder_path = os.getenv('WATCH_FOLDER', './data')
        processed_files, errors = process_folder(folder_path)
        
        # Send email report
        if send_ticket_report(env_vars, processed_files, errors):
            print("Email report sent successfully")
        else:
            print("Failed to send email report")
            
    except Exception as e:
        print(f"Application error: {str(e)}")

if __name__ == "__main__":
    main()    