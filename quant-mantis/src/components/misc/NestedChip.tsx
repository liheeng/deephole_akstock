import Chip from '@mui/material/Chip';
import { styled } from '@mui/material';

const NestedChipRoot = styled(Chip)(({ theme }) => ({
  height: 'auto',
  minWidth: '56px', // 👈 最小宽度，避免变圆
  whiteSpace: 'nowrap',
  '& .MuiChip-label': {
    display: 'inline-flex',
    alignItems: 'center',
    gap: theme.spacing(0.5),
    padding: '2px 0',
  },
}));

const NestedChip = (props: any) => {
  return <NestedChipRoot {...props} />;
};

export default NestedChip;