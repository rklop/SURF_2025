import os
import openai
from dotenv import load_dotenv

load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")

# Initialize OpenAI client
client = openai.OpenAI()

def collect_response(prompt, stop=None, max_tokens=1024):
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that generates SQL queries."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=max_tokens,
            stop=stop
        )
        output = response.choices[0].message.content.strip()

        # --- Optional debugging ---
        print("\n================== PROMPT ==================")
        #print(prompt)
        print("\n================== RESPONSE ==================")
        #print(output)
        print("\n=============================================")

        return output

    except Exception as e:
        print(f"Exception in collect_response: {e}")
        return ""