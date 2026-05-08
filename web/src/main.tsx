import React from 'react';
import { createRoot } from 'react-dom/client';
import 'ol/ol.css';
import './styles.css';
import { App } from './App';

createRoot(document.querySelector('#app') as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
