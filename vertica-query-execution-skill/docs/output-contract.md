# Output Contract

## JSON Export
{
  "meta": {
    "generated_at": "<ISO8601>",
    "source": "vertica",
    "row_count": <int>,
    "truncated": <bool>,
    "columns": [...]
  },
  "records": [{...}]
}

## CSV Export
- Header row = column names
- Null = empty
- Proper CSV escaping
- Saved under exports/vertica/
