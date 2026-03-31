fetch('http://localhost:5000/api/telemetry')
  .then(response => response.json())
  .then(data => {
    const container = document.getElementById('data-container');
    data.forEach(entry => {
      const div = document.createElement('div');
      div.innerHTML = `
        <strong>${entry.room}</strong><br/>
        Time: ${entry.timestamp}<br/>
        Temperature: ${entry.temperature} °C<br/>
        Humidity: ${entry.humidity} %<hr/>
      `;
      container.appendChild(div);
    });
  });
