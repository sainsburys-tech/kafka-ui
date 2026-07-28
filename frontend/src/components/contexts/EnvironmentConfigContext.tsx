import React from 'react';
import { useEnvironmentConfig } from 'lib/hooks/api/environmentConfig';

interface EnvironmentConfigContextProps {
  label: string;
  color: string;
}

export const EnvironmentConfigContext =
  React.createContext<EnvironmentConfigContextProps>({
    label: 'LOCAL',
    color: 'rgb(0, 71, 255)',
  });

export const EnvironmentConfigProvider: React.FC<
  React.PropsWithChildren<unknown>
> = ({ children }) => {
  const { data } = useEnvironmentConfig();

  const value = React.useMemo(
    () => ({
      label: data?.label ?? 'LOCAL',
      color: data?.color ?? 'rgb(0, 71, 255)',
    }),
    [data]
  );

  return (
    <EnvironmentConfigContext.Provider value={value}>
      {children}
    </EnvironmentConfigContext.Provider>
  );
};

