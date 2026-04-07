import { Link } from 'react-router-dom';
import { Button } from '@/components/ui/button';

export default function HomePage() {
  return (
    <div className="flex flex-col items-center justify-center min-h-screen p-8 bg-zinc-50 dark:bg-zinc-950">
      <div className="max-w-xl text-center space-y-6">
        <h1 className="text-4xl font-extrabold tracking-tight">API Testing Gateway</h1>
        <p className="text-zinc-500 text-lg">
          A minimalist interface to test the Flash Loan Detection API endpoint.
        </p>
        <Link to="/decode">
          <Button size="lg" className="mt-4">
            Test /decode API
          </Button>
        </Link>
      </div>
    </div>
  );
}
