import os
import time
from tqdm import tqdm
import google.generativeai as genai

genai.configure(api_key=os.environ["GEMINI_API_KEY"])

model = genai.GenerativeModel('gemini-2.5-flash')

def connect_gpt4(message, prompt):
    """
    using gemini instead of gpt-4
    """
    try:
        response = model.generate_content(
            f"{message}\n\nUser: {prompt}\n\nAssistant:",
            generation_config=genai.types.GenerationConfig(
                temperature=0,
                max_output_tokens = 4000,
                top_p=1,
                top_k=1,
            )
        )
        return response.text
    except Exception as e:
        print(f"Error in connect_gpt4: {e}")
        return ""

def collect_response(prompt, max_tokens = 4000, stop=None):

    max_retries = 3
    retry_count = 0

    print()
    
    while retry_count < max_retries:
        try:

            full_prompt = f"You are an AI assistant that helps people find information.\n\nUser: {prompt}\n\nAssistant:"
            
            print(f'\n{'='*60}')
            #print(f"Prompt: {full_prompt}")
            print(f"{'-'*60}")
            
            response = model.generate_content(
                full_prompt,
                generation_config = genai.types.GenerationConfig(
                    temperature = 0,
                    max_output_tokens = max_tokens, #4000
                    top_p=1,
                    top_k=1,
                )
            )
        
            if response.candidates and response.candidates[0].finish_reason == 2:
                print(f" Hit token limit (max_tokens = {max_tokens}). Retry {retry_count + 1}/{max_retries}")

                retry_count += 1
                time.sleep(2)

                continue
            
            try:
                response_text = response.text
                print(f'Response: {response_text}')

            except Exception as text_error:
                print(f"Error getting response.text: {text_error}")
                print(f"Response object: {response}")

                retry_count += 1

                time.sleep(1)
                continue
            
            print(f"{'='*60}")
            return response_text
            
        except Exception as e:
            print(f"Exception in collect_response: {e}")
            print(f"Exception type: {type(e)}")

            retry_count += 1
            time.sleep(1)
    
    print(" max retries reached. returning empty response.")
    return ""
