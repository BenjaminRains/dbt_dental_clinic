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
                        manualChunks: {
                            // Separate vendor chunks for better caching
                            'react-vendor': ['react', 'react-dom', 'react-router-dom'],
                            'mui-vendor': ['@mui/material', '@mui/icons-material', '@emotion/react', '@emotion/styled'],
                            'chart-vendor': ['recharts'],
                            'utils-vendor': ['axios', 'zustand']
                        }
                    }
                },
                chunkSizeWarningLimit: 600 // Increase limit slightly since we're code-splitting
            }
        })
    }
})
