# Frontend Setup and Execution Guide

This document provides instructions on how to set up and run the Vite + React frontend for the Flash-Loan Attack Detection System (Tech-Noir Dashboard).

## Prerequisites
- Node.js (version 18 or higher recommended)
- npm (Node Package Manager)

## 1. Install Dependencies
Before running the application for the first time, you must install all the required Node packages. 

Open your terminal, ensure you are inside the `frontend` directory, and run:

```bash
npm install
```

This will read the `package.json` file and install all core dependencies (React, Vite, TailwindCSS) and component libraries (Recharts, React Flow, Lucide-React).

## 2. Run the Development Server
To start the application in development mode with Hot-Module-Replacement (HMR), run the following command:

```bash
npm run dev
```

The Vite development server will start immediately. You can view the dashboard by opening your web browser and navigating to:

**http://localhost:5173**

## 3. Build for Production
When you are ready to deploy the application to a production environment, run:

```bash
npm run build
```

This command will optimize your assets and bundle the React application into the `dist` directory. You can then serve this `dist` folder using any static file server (such as Nginx, Vercel, or Netlify).

## 4. Linting and Type Checking
To ensure your code follows the established style guidelines and has no TypeScript errors, you can run the linter:

```bash
npm run lint
```
