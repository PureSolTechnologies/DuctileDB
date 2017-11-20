import React from 'react';

import Menu from './Menu';
import Footer from './Footer';
import Header from './Header';

export default function Layout( { children } ) {
    return (
        <div className="container">
            <Header />
            <Menu />
            {children}
            <Footer />
        </div>
    );
}
