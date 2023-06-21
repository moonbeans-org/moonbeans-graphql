import openai

# Set your OpenAI API key
openai.api_key = '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'

# Test API call
def test_openai_key():
    try:
        response = openai.Completion.create(
            engine='davinci',
            prompt='Hello, OpenAI!',
            max_tokens=5
        )
        return response['choices'][0]['text'].strip()
    except Exception as e:
        return str(e)

# Test the OpenAI key
result = test_openai_key()

# Check the result
if isinstance(result, str):
    print("OpenAI key is working!")
else:
    print("OpenAI key test failed. Please check your API key.")
