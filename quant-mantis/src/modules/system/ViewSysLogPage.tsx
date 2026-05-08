import { useEffect, useRef, useState } from "react";
import {
  Box,
  Paper,
  Typography,
  TextField,
  Select,
  MenuItem,
  Stack,
  Button,
  Slider,
} from "@mui/material";
import {
  fetchDefaultLogs,
  createSystemLogWebsockChannel,
} from "../../api/Client";

interface LogLine {
  type?: string;
  timestamp: string;
  level: string;
  message: string;
}

export default function ViewSysLogPage() {
  const [logs, setLogs] = useState<LogLine[]>([]);
  const [filterKeyword, setFilterKeyword] = useState("");
  const [filterLevel, setFilterLevel] = useState("");
  const [running, setRunning] = useState(false);

  // 字号
  const [fontSize, setFontSize] = useState<number>(16);

  // 搜索
  const [searchText, setSearchText] = useState("");
  const [searchIndex, setSearchIndex] = useState<number>(-1);

  const wsRef = useRef<WebSocket | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // 每条日志 DOM 引用
  const logRefs = useRef<(HTMLDivElement | null)[]>([]);

  // 初次加载历史日志
  useEffect(() => {
    const loadLogs = async () => {
      const res = await fetchDefaultLogs(200);
      setLogs(res);
    };

    loadLogs();
  }, []);

  // WebSocket 实时日志
  useEffect(() => {
    if (!running) return;

    let mounted = true;

    createSystemLogWebsockChannel().then((ws) => {
      if (!mounted || !ws) return;

      wsRef.current = ws;

      ws.onopen = () => {
        console.log("日志 WebSocket 已连接");
      };

      ws.onmessage = (event) => {
        const data: LogLine = JSON.parse(event.data);

        setLogs((prev) => [...prev, data]);
      };

      ws.onclose = () => {
        console.log("日志 WebSocket 已关闭");
      };
    });

    return () => {
      mounted = false;

      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [running]);

  // 自动滚动到底部（仅非搜索状态）
  useEffect(() => {
    if (searchText) return;

    if (containerRef.current) {
      containerRef.current.scrollTop =
        containerRef.current.scrollHeight;
    }
  }, [logs, searchText]);

  // 日志过滤
  const filteredLogs = logs.filter(
    (l) =>
      (!filterLevel || l.level === filterLevel) &&
      (!filterKeyword ||
        l.message
          .toLowerCase()
          .includes(filterKeyword.toLowerCase()))
  );

  // 搜索匹配索引
  const matchedIndexes = filteredLogs
    .map((log, idx) =>
      log.message
        .toLowerCase()
        .includes(searchText.toLowerCase())
        ? idx
        : -1
    )
    .filter((idx) => idx !== -1);

  // 滚动到指定日志
  const scrollToMatch = (index: number) => {
    const el = logRefs.current[index];

    if (el) {
      el.scrollIntoView({
        behavior: "smooth",
        block: "center",
      });
    }
  };

  // 下一个匹配
  const handleNextSearch = () => {
    if (matchedIndexes.length === 0) return;

    const next =
      searchIndex + 1 >= matchedIndexes.length
        ? 0
        : searchIndex + 1;

    setSearchIndex(next);

    scrollToMatch(matchedIndexes[next]);
  };

  // 上一个匹配
  const handlePrevSearch = () => {
    if (matchedIndexes.length === 0) return;

    const prev =
      searchIndex - 1 < 0
        ? matchedIndexes.length - 1
        : searchIndex - 1;

    setSearchIndex(prev);

    scrollToMatch(matchedIndexes[prev]);
  };

  // 日志颜色
  const getLogColor = (level: string) => {
    const lv = level.toUpperCase();

    if (lv.includes("ERROR")) return "#f44336";
    if (lv.includes("WARN")) return "#ffeb3b";
    if (lv.includes("INFO")) return "#ffffff";
    if (lv.includes("DEBUG")) return "#9e9e9e";

    return "#ffffff";
  };

  // 高亮搜索关键字
  const renderHighlightedText = (text: string) => {
    if (!searchText) return text;

    const lowerText = text.toLowerCase();
    const lowerSearch = searchText.toLowerCase();

    const parts = [];
    let lastIndex = 0;

    while (true) {
      const index = lowerText.indexOf(lowerSearch, lastIndex);

      if (index === -1) {
        parts.push(text.slice(lastIndex));
        break;
      }

      // 普通文本
      parts.push(text.slice(lastIndex, index));

      // 高亮文本
      parts.push(
        <span
          key={index}
          style={{
            backgroundColor: "#ff9800",
            color: "#000",
            borderRadius: 2,
            padding: "0 2px",
          }}
        >
          {text.slice(index, index + searchText.length)}
        </span>
      );

      lastIndex = index + searchText.length;
    }

    return parts;
  };

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      {/* 工具栏 */}
      <Stack
        direction="row"
        spacing={2}
        sx={{ mb: 1, flexWrap: "wrap" }}
        alignItems="center"
      >
        {/* 过滤关键字 */}
        <TextField
          size="small"
          label="过滤关键字"
          value={filterKeyword}
          onChange={(e) =>
            setFilterKeyword(e.target.value)
          }
        />

        {/* 日志级别 */}
        <Select
          size="small"
          value={filterLevel}
          onChange={(e) =>
            setFilterLevel(e.target.value)
          }
          displayEmpty
        >
          <MenuItem value="">全部</MenuItem>
          <MenuItem value="INFO">INFO</MenuItem>
          <MenuItem value="ERROR">ERROR</MenuItem>
          <MenuItem value="DEBUG">DEBUG</MenuItem>
          <MenuItem value="WARN">WARN</MenuItem>
        </Select>

        {/* 实时日志 */}
        <Button
          variant="contained"
          color={running ? "error" : "primary"}
          onClick={() => setRunning((r) => !r)}
        >
          {running ? "停止实时日志" : "开始实时日志"}
        </Button>

        {/* 搜索 */}
        <TextField
          size="small"
          label="搜索日志"
          value={searchText}
          onChange={(e) => {
            setSearchText(e.target.value);
            setSearchIndex(-1);
          }}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              handleNextSearch();
            }
          }}
        />

        <Button
          variant="outlined"
          onClick={handlePrevSearch}
          disabled={matchedIndexes.length === 0}
        >
          上一个
        </Button>

        <Button
          variant="outlined"
          onClick={handleNextSearch}
          disabled={matchedIndexes.length === 0}
        >
          下一个
        </Button>

        {/* 搜索结果数量 */}
        <Typography
          variant="body2"
          sx={{
            color: "#999",
            minWidth: 80,
          }}
        >
          {matchedIndexes.length > 0
            ? `${searchIndex + 1}/${matchedIndexes.length}`
            : "0/0"}
        </Typography>

        {/* 字号调节 */}
        <Box
          sx={{
            width: 200,
            display: "flex",
            alignItems: "center",
          }}
        >
          <Typography
            variant="body2"
            sx={{
              mr: 1,
              whiteSpace: "nowrap",
            }}
          >
            字号：{fontSize}px
          </Typography>

          <Slider
            size="small"
            min={10}
            max={32}
            step={1}
            value={fontSize}
            onChange={(_, value) =>
              setFontSize(value as number)
            }
            valueLabelDisplay="auto"
          />
        </Box>
      </Stack>

      {/* 日志区域 */}
      <Paper
        ref={containerRef}
        sx={{
          flex: 1,
          p: 2,
          overflowY: "auto",
          backgroundColor: "#1e1e1e",
          fontFamily: "monospace",
          whiteSpace: "pre-wrap",
          textAlign: "left",
        }}
      >
        {filteredLogs.length === 0 ? (
          <Typography
            sx={{
              color: "gray",
              fontStyle: "italic",
            }}
          >
            暂无日志
          </Typography>
        ) : (
          filteredLogs.map((line, idx) => {
            const isMatched =
              searchText &&
              line.message
                .toLowerCase()
                .includes(searchText.toLowerCase());

            const isCurrent =
              matchedIndexes[searchIndex] === idx;

            return (
              <div
                key={idx}
                ref={(el) => {
                  logRefs.current[idx] = el;
                }}
              >
                <Typography
                  sx={{
                    color: getLogColor(line.level),
                    lineHeight: 1.5,
                    whiteSpace: "pre-wrap",
                    fontSize: `${fontSize}px`,
                    backgroundColor: isCurrent
                      ? "#264f78"
                      : isMatched
                      ? "rgba(255,255,0,0.08)"
                      : "transparent",
                    transition: "0.2s",
                    borderRadius: 1,
                    px: 0.5,
                  }}
                >
                  {line.type === "__CONTENT__"
                    ? renderHighlightedText(
                        line.message
                      )
                    : renderHighlightedText(
                        `[${line.timestamp}] [${line.level}] ${line.message}`
                      )}
                </Typography>
              </div>
            );
          })
        )}
      </Paper>
    </Box>
  );
}