import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig(({ command, mode }) => {
    const isDev = command === 'serve'
    const isProduction = command === 'build'

    return {
        plugins: [react()],

        // Optimize dependencies - pre-bundle React and scheduler together
        // CRITICAL: scheduler must be included to prevent "unstable_now" errors
        // React's scheduler is an internal dependency that must stay with React
        optimizeDeps: {
            include: [
                'react',
                'react-dom',
                'react/jsx-runtime',
                'react-router-dom',
                'scheduler', // React's internal scheduler - must be pre-bundled
            ],
        },

        // Development server config (only in dev)
        ...(isDev && {
            server: {
                port: 3000,
                proxy: {
                    '/api': {
                        target: 'http://localhost:8000',
                        changeOrigin: true,
                        rewrite: (path) => path.replace(/^\/api/, '')
                    }
                }
            }
        }),

        // Build config (only in production)
        ...(isProduction && {
            build: {
                outDir: 'dist',
                sourcemap: false, // Disabled for production (security & performance)
                rollupOptions: {
                    output: {
                        entryFileNames: 'assets/[name]-[hash].js',
                        chunkFileNames: 'assets/[name]-[hash].js',
                        manualChunks: (id) => {
                            // Node modules chunking strategy
                            // WHITELIST APPROACH: Only known independent libraries are separate
                            // Everything else (including unknown dependencies) goes with React
                            // This prevents initialization errors from transitive dependencies
                            if (id.includes('node_modules')) {
                                // CRITICAL: Don't chunk React core or scheduler - keep in entry bundle
                                // React core and scheduler are small and must be available before any other code
                                // This ensures React is ALWAYS loaded first, preventing initialization errors
                                // scheduler is React's internal dependency - must stay with React core
                                if (id.includes('/react/') &&
                                    !id.includes('react-dom') &&
                                    !id.includes('react-router') &&
                                    !id.includes('react/jsx-runtime')) {
                                    // Return undefined to keep React core in entry bundle
                                    return undefined;
                                }
                                // CRITICAL: scheduler must stay with React - don't chunk it separately
                                // React's scheduler API (unstable_now) must be available when React loads
                                if (id.includes('scheduler')) {
                                    // Keep scheduler with React core in entry bundle
                                    return undefined;
                                }

                                // WHITELIST: Known independent libraries (safe to separate)
                                // Only these are guaranteed to NOT depend on React
                                if (id.includes('axios')) {
                                    return 'utils-vendor';
                                }
                                if (id.includes('zustand')) {
                                    return 'utils-vendor';
                                }
                                if (id.includes('mermaid')) {
                                    // Mermaid is dynamically imported and independent
                                    return 'mermaid-vendor';
                                }

                                // DEFAULT: Bundle everything else with React
                                // This includes:
                                // - React DOM, Router, jsx-runtime
                                // - MUI Material, Icons
                                // - Emotion
                                // - Recharts
                                // - Any transitive dependencies we don't know about
                                // - Any future React libraries you add
                                //
                                // Why this approach?
                                // 1. Prevents initialization errors (err on side of caution)
                                // 2. Transitive dependencies are automatically included
                                // 3. Future-proof (new React libraries work automatically)
                                // 4. Simpler maintenance (no need to update config for each library)
                                return 'react-vendor';
                            }
                        }
                    }
                },
                chunkSizeWarningLimit: 500 // Keep warning at reasonable level to catch issues
            }
        })
    }
})
