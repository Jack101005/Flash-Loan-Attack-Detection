import { Routes, Route } from 'react-router-dom';
import HomePage from './pages/HomePage';
import DecodePage from './pages/DecodePage';

function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/decode" element={<DecodePage />} />
    </Routes>
  )
}

export default App
