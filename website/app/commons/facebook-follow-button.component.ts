import {Component} from 'angular2/core';

@Component({
  selector: 'facebook-follow-button',
  template: `
<div class="fb-follow" data-href="https://www.facebook.com/puresoltechnologies/" data-layout="standard" data-show-faces="true"></div>
    `  
})
export class FacebookFollowButtonComponent {
  ngAfterContentInit() {
//    FB.XFBML.parse();
  }
}
