Write-Host "=== Starting QueueCTL Demo ==="

# Clean start
Remove-Item queuectl.db -Force -ErrorAction SilentlyContinue

# Start worker in background
Start-Process powershell -ArgumentList "queuectl worker start --count 1"

Start-Sleep -Seconds 1

# Successful job
@'
{
  "id": "hello1",
  "command": "echo Hello Success Demo"
}
'@ | Out-File job1.json -Encoding UTF8

queuectl enqueue --file job1.json

Start-Sleep -Seconds 2
queuectl status

# Failing job (will retry then DLQ)
@'
{
  "id": "fail1",
  "command": "bash -c \"exit 1\"",
  "max_retries": 2
}
'@ | Out-File job2.json -Encoding UTF8

queuectl enqueue --file job2.json

Write-Host "Waiting for retries..."
Start-Sleep -Seconds 10

queuectl dlq list

Write-Host "Requeueing failed job..."
queuectl dlq retry fail1

Start-Sleep -Seconds 4
queuectl status

Write-Host "=== Demo Complete ==="
