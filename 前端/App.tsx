import React from 'react';
import UserData from './UserData';

const App: React.FC = () => {
  return (
    <div className="App">
      <UserData userId="123" />
    </div>
  );
};

export default App;