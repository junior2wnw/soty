import React from 'react';
import { createRoot } from 'react-dom/client';
import '@fontsource-variable/golos-text/wght.css';
import App from './App';
import './styles.css';
import './timeline.css';
import './universal.css';

createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
