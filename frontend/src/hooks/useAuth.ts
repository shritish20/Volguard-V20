import { useState, useCallback, useEffect } from 'react'

export function useAuth() {
  const [token, setToken] = useState(() => localStorage.getItem('upstox_token'))
  const [isValid, setIsValid] = useState(!!localStorage.getItem('upstox_token'))

  useEffect(() => {
    const handler = () => { setToken(null); setIsValid(false) }
    window.addEventListener('auth:logout', handler)
    return () => window.removeEventListener('auth:logout', handler)
  }, [])

  const login = useCallback((t: string) => {
    const trimmed = t.trim()
    localStorage.setItem('upstox_token', trimmed)
    setToken(trimmed)
    setIsValid(true)
  }, [])

  const logout = useCallback(() => {
    localStorage.removeItem('upstox_token')
    setToken(null)
    setIsValid(false)
  }, [])

  return { isAuthenticated: !!token && isValid, token, login, logout }
}
