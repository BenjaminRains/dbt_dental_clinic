import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig(({ command, mode }) => {
    const isDev = command === 'serve'
    const isProduction = command === 'build'

    return {
        plugins: [react()],

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
                        manualChunks: (id) => {
                            // Node modules chunking strategy
                            if (id.includes('node_modules')) {
                                // React core libraries
                                if (id.includes('react') || id.includes('react-dom') || id.includes('react-router')) {
                                    return 'react-vendor';
                                }

                                // MUI icons are large - separate them
                                if (id.includes('@mui/icons-material')) {
                                    return 'mui-icons';
                                }

                                // MUI core (material + emotion)
                                if (id.includes('@mui/material') || id.includes('@emotion')) {
                                    return 'mui-core';
                                }

                                // Charting library
                                if (id.includes('recharts')) {
                                    return 'chart-vendor';
                                }

                                // Mermaid (large library, already dynamically imported)
                                if (id.includes('mermaid')) {
                                    return 'mermaid-vendor';
                                }

                                // Utility libraries
                                if (id.includes('axios') || id.includes('zustand')) {
                                    return 'utils-vendor';
                                }

                                // All other node_modules go into a separate vendor chunk
                                return 'vendor';
                            }
                        }
                    }
                },
                chunkSizeWarningLimit: 500 // Keep warning at reasonable level to catch issues
            }
        })
    }
})
