import { Routes, Route } from 'react-router-dom';
import { DashboardLayout } from '@/components/layout/DashboardLayout';
import HomePage from './pages/HomePage';
import DecodePage from './pages/DecodePage';
import PipelineSimulatorPage from './pages/PipelineSimulatorPage';
import DemoPage from './pages/DemoPage';

function App() {
  return (
    <Routes>
      <Route element={<DashboardLayout />}>
        <Route path="/" element={<HomePage />} />
        <Route path="/pipeline-simulator" element={<PipelineSimulatorPage />} />
        <Route path="/decode" element={<DecodePage />} />
        <Route path="/demo" element={<DemoPage />} />
      </Route>
    </Routes>
  )
}

export default App
