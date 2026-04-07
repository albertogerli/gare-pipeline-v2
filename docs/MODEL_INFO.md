# Model Configuration

## Default LLM Model

This project uses **gpt-5-mini** as the default language model for all AI operations.

### Configuration

The model is configured via environment variables:

```bash
# In your .env file
OPENAI_API_KEY=your_api_key_here
MINI_MODEL=gpt-5-mini
```

### Model Characteristics

- **Model Name**: gpt-5-mini
- **Temperature**: 0 (for deterministic outputs)
- **Purpose**: Text analysis, entity extraction, categorization
- **JSON Mode**: Enabled for structured outputs

### Usage

The model is automatically used by:
- `GazzettaAnalyzer` - for analyzing tender texts
- `OCDSAnalyzer` - for processing OCDS data
- All other analysis modules

### Override

To use a different model, set the `MINI_MODEL` environment variable:

```bash
export MINI_MODEL=gpt-5-turbo  # Example of different model
```

Or pass it directly when initializing the LLM wrapper:

```python
from src.utils.llm_wrapper import LLMWrapper

llm = LLMWrapper(model="gpt-5-turbo")
```