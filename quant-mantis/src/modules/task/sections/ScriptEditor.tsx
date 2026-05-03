import React, { useState } from 'react';
import { Box, IconButton, Select, MenuItem, Tooltip } from '@mui/material';
import Editor, { loader } from '@monaco-editor/react';
import { PlayArrow, Stop } from '@mui/icons-material';

// 可选内置主题：vs (浅色), vs-dark (深色), hc-black (高对比)
const THEMES = ['vs-dark', 'vs', 'hc-black'] as const;
type ThemeType = typeof THEMES[number];

interface ScriptEditorProps {
  value: string;
  onChange: (v: string) => void;
  onRun?: () => void;
  onStop?: () => void;
}

export default function ScriptEditor({ value, onChange, onRun, onStop }: ScriptEditorProps) {
  const [fontSize, setFontSize] = useState<number>(14);
  const [theme, setTheme] = useState<ThemeType>('vs-dark');

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      
      {/* 工具条 */}
      <Box sx={{ display: 'flex', alignItems: 'center', mb: 1, gap: 1 }}>
        <Tooltip title="Run Script">
          <IconButton onClick={onRun}><PlayArrow /></IconButton>
        </Tooltip>
        <Tooltip title="Stop Script">
          <IconButton onClick={onStop}><Stop /></IconButton>
        </Tooltip>

        <Select
          value={fontSize}
          onChange={(e) => setFontSize(Number(e.target.value))}
          size="small"
        >
          {[12, 14, 16, 18, 20].map(size => (
            <MenuItem key={size} value={size}>{size}px</MenuItem>
          ))}
        </Select>

        <Select
          value={theme}
          onChange={(e) => setTheme(e.target.value as ThemeType)}
          size="small"
        >
          {THEMES.map(t => <MenuItem key={t} value={t}>{t}</MenuItem>)}
        </Select>
      </Box>

      {/* 编辑器 */}
      <Box sx={{ flex: 1 }}>
        <Editor
          height="100%"
          defaultLanguage="python"
          value={value}
          onChange={onChange}
          theme={theme}
          options={{
            fontSize,
            minimap: { enabled: false },
            automaticLayout: true,
            scrollBeyondLastLine: false,
            renderWhitespace: 'boundary',
            wordWrap: 'on',
          }}
        />
      </Box>
    </Box>
  );
}