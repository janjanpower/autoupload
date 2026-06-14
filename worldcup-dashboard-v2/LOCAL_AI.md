# Local Qwen research setup

This feature is creator-only. It is hidden on the public GitHub Pages site and only appears when the dashboard is opened from `localhost`, `127.0.0.1`, or `::1`.

## Start the local services

Open two terminals from the repository root.

Terminal 1:

```powershell
python -m http.server 8765
```

Open:

```text
http://localhost:8765/worldcup-dashboard-v2/index.html
```

Terminal 2:

```powershell
python worldcup-dashboard-v2/scripts/local_research_server.py
```

Keep Ollama running with the model used by the dashboard:

```powershell
ollama pull qwen2.5:3b-instruct
```

The dashboard calls:

- Ollama: `http://localhost:11434/api/chat`
- Local research: `http://127.0.0.1:8777/api/research`

## What the research server does

Before sending the prompt to Qwen, the dashboard asks the local research server for public web snippets about:

- matchup previews
- injuries
- suspensions
- yellow/red cards
- team news
- Chinese-language matchup context

The server returns raw search snippets. Qwen is instructed to treat them as unverified context and to keep actual standings separate from the scenario score.

## Failure behavior

If the research server is not running, Qwen analysis still works with:

- dashboard standings
- scenario impact
- manually pasted context

The page will show that automatic research failed and continue the Qwen request.
