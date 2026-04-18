import { forwardRef } from 'react';
import type {ReactNode} from 'react';

// MUI components
import { useTheme } from '@mui/material/styles';
import { Card, CardContent, CardHeader, Divider, Typography } from '@mui/material';

// 定义组件属性接口
export interface MainCardProps {
  border?: boolean;
  shadow?: string;
  children: ReactNode;
  content?: boolean; // 是否自动包装 CardContent
  contentSX?: object;
  darkTitle?: boolean;
  divider?: boolean;
  sx?: object;
  title?: string | ReactNode;
  elevation?: number;
  secondary?: ReactNode; // 标题栏右侧的额外操作区域
}

const MainCard = forwardRef<HTMLDivElement, MainCardProps>(
  (
    {
      border = true,
      shadow,
      children,
      content = true,
      contentSX = {},
      darkTitle,
      divider = true,
      elevation,
      secondary,
      sx = {},
      title,
      ...others
    },
    ref
  ) => {
    const theme = useTheme();

    return (
      <Card
        elevation={elevation || 0}
        ref={ref}
        {...others}
        sx={{
          border: border ? '1px solid' : 'none',
          borderRadius: 2,
          borderColor: theme.palette.mode === 'dark' ? theme.palette.divider : theme.palette.grey[200],
          boxShadow: shadow || 'none',
          '& pre': {
            m: 0,
            p: '16px !important',
            fontFamily: theme.typography.fontFamily,
            fontSize: '0.75rem'
          },
          ...sx
        }}
      >
        {/* 渲染卡片头部 */}
        {title && (
          <CardHeader
            sx={{
              p: 2.5,
              '& .MuiCardHeader-action': { m: '0px auto', alignSelf: 'center' }
            }}
            title={
              typeof title === 'string' ? (
                <Typography sx={{variant:"h5", fontWeight:600}}>
                  {title}
                </Typography>
              ) : (
                title
              )
            }
            action={secondary}
          />
        )}

        {/* 标题与内容之间的分隔线 */}
        {title && divider && <Divider />}

        {/* 渲染内容区域 */}
        {content ? (
          <CardContent sx={{ p: 2.5, ...contentSX }}>
            {children}
          </CardContent>
        ) : (
          children
        )}
      </Card>
    );
  }
);

export default MainCard;